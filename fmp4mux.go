package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"sync"
)

// fmp4Muxer re-muxes raw H.264 Annex-B access units into a live fragmented
// MP4 (fMP4) byte stream for the /stream MSE fallback path. The raw path
// (WebCodecs over /raw) is the primary; this muxer exists so browsers without
// WebCodecs can still watch via MediaSource/SourceBuffer.
//
// Output layout (matches what the client's startStreamPipeline expects):
//
//	ftyp + moov (init segment, written once)
//	moof + mdat (fragment pairs, written as frames arrive)
//
// The moov carries an avcC box built from the first SPS+PPS pair the muxer
// sees, so the client can parse the codec string from the init segment
// exactly as it does today. Fragments are cut at IDR boundaries (so the
// client's live-edge seek lands on a keyframe present in the buffer) or
// after maxFragmentFrames frames, whichever comes first.
//
// Timestamps are CFR (outFPS): each access unit is one frame of
// frameDur = 1000000/outFPS track units (timescale 1,000,000 = 1 microsecond
// per unit, the standard H.264 timescale). The client plays in SourceBuffer
// "sequence" mode, which ignores the tfdt base time, so the per-fragment
// base time is 0 and only the per-sample durations matter. The 32-bit
// track time wraps at 2^32 (about 13.6 hours at 30fps); sequence mode makes
// the wrap invisible to the client.
//
// The muxer is NOT goroutine-safe for feed; feed is called only from the raw
// pump goroutine. Close is mutex-guarded because teardown may race the pump's
// final Close. The emit callback (pushing box groups to rs.stream) is
// invoked synchronously from feed/Close.
type fmp4Muxer struct {
	outFPS int    // clean output framerate (CFR)
	emit   func([]byte) // box-group sink (rs.stream.write)

	mu sync.Mutex // guards Close racing feed on teardown

	initEmitted bool  // ftyp+moov has been written
	sps, pps    []byte // first SPS/PPS (with 4-byte start codes), for avcC
	width, height int  // track dimensions (tkhd)

	// fragment assembly state.
	fragStart   uint32 // track time (1us units) of the first sample in the pending fragment
	fragSamples int    // access units accumulated in the pending fragment
	fragIDR     bool   // pending fragment contains an IDR (keyframe-aligned)
	pending     []byte // raw Annex-B bytes of the pending fragment's samples
	sampleSizes []uint32 // per-sample raw sizes (trun)

	// frameCount is the number of access units seen so far; the next sample's
	// DTS is frameCount * frameDur (track units).
	frameCount uint32
}

// newFmp4Muxer creates a muxer for an outFPS-framerate stream; emit receives
// each complete box group (init segment or moof+mdat fragment) as it is
// produced.
func newFmp4Muxer(outFPS int, emit func([]byte)) *fmp4Muxer {
	return &fmp4Muxer{outFPS: outFPS, emit: emit}
}

// frameDur returns the per-sample duration in track units (timescale
// 1,000,000).
func (m *fmp4Muxer) frameDur() uint32 {
	return 1000000 / uint32(m.outFPS)
}

// noteSPSPPS extracts any SPS/PPS NAL units (type 7/8) from the AU. h264parse
// re-emits them before each IDR, so the first AU carries them; later AUs may
// too (harmless - we only keep the first pair). This is the cheap, cheap-to-
// skip part of feed: the pump calls it for every AU so the muxer always has
// the first SPS/PPS pair and can emit the init segment (ftyp+moov) the moment
// the first /stream client joins, even if no /stream client was connected
// when the stream started.
func (m *fmp4Muxer) noteSPSPPS(au []byte) {
	buf := au
	for {
		sc := findStartCode(buf)
		if sc < 0 {
			break
		}
		nal := buf[sc:]
		if len(nal) >= 4 && nal[0] == 0 && nal[1] == 0 && nal[2] == 0 && nal[3] == 1 {
			nal = nal[4:]
		} else if len(nal) >= 3 && nal[0] == 0 && nal[1] == 0 && nal[2] == 1 {
			nal = nal[3:]
		}
		if len(nal) == 0 {
			break
		}
		switch nal[0] & 0x1F {
		case 7: // SPS
			if m.sps == nil {
				m.sps = append([]byte{0, 0, 0, 1}, nal...)
			}
		case 8: // PPS
			if m.pps == nil {
				m.pps = append([]byte{0, 0, 0, 1}, nal...)
			}
		}
		// Advance past this NAL unit to the next start code (Annex-B: the
		// next NAL begins at the next start code).
		next := findStartCodeAfter(nal, 0)
		if next < 0 {
			break
		}
		buf = nal[next:]
	}
}

// bumpFrame advances the frame counter for one access unit. The pump calls it
// for every AU (gated or not) so the fragment DTS (frameCount * frameDur)
// tracks the true frame index; the client plays in sequence mode, which
// ignores the absolute base time, so the exact value only matters for the
// mfhd sequence number.
func (m *fmp4Muxer) bumpFrame() {
	m.frameCount++
}

// feed processes one raw H.264 Annex-B access unit for the fMP4 fallback
// path: it emits the init segment (ftyp+moov) once the first SPS+PPS pair is
// known (noteSPSPPS must have been called for a prior AU carrying them),
// appends the AU to the pending fragment, and emits the fragment (moof+mdat)
// when it is keyframe-aligned (contains an IDR) or has reached
// maxFragmentFrames samples. The pump calls feed only while at least one
// /stream client is connected (the fMP4 path is the fallback; building moof
// + mdat boxes for nobody costs CPU and bandwidth the primary raw path does
// not need). noteSPSPPS + bumpFrame are called unconditionally so a client
// that joins mid-stream still receives the init segment and a fragment whose
// DTS reflects the true frame index.
//
// feed does NOT advance frameCount (the pump's bumpFrame does, unconditionally)
// and does NOT extract SPS/PPS (noteSPSPPS does, unconditionally).
func (m *fmp4Muxer) feed(au []byte) {
	// Emit the init segment (ftyp+moov) once the first SPS+PPS pair is known
	// (captured by noteSPSPPS on an earlier AU).
	if !m.initEmitted && m.sps != nil && m.pps != nil {
		m.width, m.height = parseSPSDimensions(m.sps)
		if m.width == 0 {
			m.width, m.height = 1920, 1080
		}
		m.emit(m.buildInitSegment())
		m.initEmitted = true
		log.Printf("fmp4mux: init segment emitted (track %dx%d, codec %s)",
			m.width, m.height, m.codecString())
	}

	// Is this AU a keyframe (contains an IDR slice, type 5)?
	isIDR := containsNalType(au, 5)

	// Append the AU to the pending fragment.
	if m.fragSamples == 0 {
		m.fragStart = m.frameCount * m.frameDur()
	}
	m.pending = append(m.pending, au...)
	m.sampleSizes = append(m.sampleSizes, uint32(len(au)))
	m.fragSamples++
	if isIDR {
		m.fragIDR = true
	}
	// NOTE: frameCount is advanced by the pump's bumpFrame() (called for every
	// AU, gated or not), NOT here - so the fragment DTS reflects the true
	// frame index even when no /stream client was connected for a while.

	// Emit the fragment when it is keyframe-aligned (contains an IDR) or has
	// reached the sample cap. A fragment that is not keyframe-aligned is
	// never emitted on its own: the client's live-edge seek needs a keyframe
	// at the fragment start, so non-IDR AUs accumulate until the next IDR
	// closes the fragment.
	if m.fragIDR || m.fragSamples >= maxFragmentFrames {
		m.emitFragment()
	}
}

// emitFragment writes the pending fragment as a moof+mdat pair and resets the
// fragment state.
func (m *fmp4Muxer) emitFragment() {
	if m.fragSamples == 0 {
		return
	}
	var frag bytes.Buffer
	frag.Write(m.buildMoof())
	// mdat: 8-byte header + the raw sample bytes.
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(8+len(m.pending)))
	copy(hdr[4:8], []byte("mdat"))
	frag.Write(hdr[:])
	frag.Write(m.pending)
	m.emit(frag.Bytes())
	// Reset the pending fragment.
	m.fragSamples = 0
	m.fragIDR = false
	m.pending = m.pending[:0]
	m.sampleSizes = m.sampleSizes[:0]
}

// Close flushes any pending fragment and marks the muxer closed. Safe to call
// more than once (teardown may race the pump's final Close).
func (m *fmp4Muxer) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fragSamples > 0 {
		m.emitFragment()
	}
}

// codecString returns the avc1.PPCCLL codec string parsed from the SPS
// (profile_idc, constraint byte, level_idc as 6 uppercase hex digits).
// Returns "" if the SPS is not yet known.
func (m *fmp4Muxer) codecString() string {
	// m.sps carries a 4-byte start code; the SPS NAL header is at offset 4,
	// so profile/constraint/level are at offsets 5/6/7.
	if len(m.sps) < 8 {
		return ""
	}
	two := func(n byte) string {
		const hex = "0123456789ABCDEF"
		return string([]byte{hex[n>>4], hex[n&0xF]})
	}
	return "avc1." + two(m.sps[5]) + two(m.sps[6]) + two(m.sps[7])
}

// buildInitSegment builds the ftyp + moov box group. The moov carries a
// single video track (track ID 1) whose avcC is built from the first
// SPS+PPS pair. The mvhd has zero duration (live stream) and a 1,000,000
// timescale; the client's SourceBuffer plays in sequence mode, so the
// absolute durations are irrelevant - only the per-fragment sample layout
// matters.
func (m *fmp4Muxer) buildInitSegment() []byte {
	var out bytes.Buffer
	// ftyp: isom brand, compatible with isom/iso2/avc1/mp41.
	ftyp := []byte{
		0, 0, 0, 20, // size
		'i', 's', 'o', 'm',
		0, 0, 0, 0, // minor version
		'i', 's', 'o', 'm', 'i', 's', 'o', '2', 'a', 'v', 'c', '1',
	}
	out.Write(ftyp)

	var moov bytes.Buffer
	// mvhd (version 0): 108 bytes total.
	{
		var b bytes.Buffer
		writeBoxHeader(&b, "mvhd", 100)
		b.Write([]byte{0, 0, 0, 0}) // version 0, flags 0
		b.Write(zero4)              // creation_time
		b.Write(zero4)              // modification_time
		b.Write(u32(1000000))      // timescale
		b.Write(zero4)              // duration (live)
		b.Write([]byte{0x00, 0x01, 0x00, 0x00}) // rate 1.0
		b.Write([]byte{0x00, 0x00})             // volume 1.0
		b.Write(make([]byte, 8))                // reserved
		// matrix (identity): 9 x 4 bytes (1.0 at 0x00010000, 0x40000000).
		b.Write([]byte{
			0x00, 0x01, 0x00, 0x00, 0, 0, 0, 0,
			0, 0, 0, 0, 0x00, 0x01, 0x00, 0x00,
			0, 0, 0, 0, 0, 0, 0x40, 0x00,
		})
		b.Write(make([]byte, 24)) // pre_defined
		b.Write(u32(2))           // next_track_ID
		moov.Write(b.Bytes())
	}
	// trak: tkhd + mdia (mdhd + hdlr + minf (vmhd + dinf + stbl)).
	{
		var trak bytes.Buffer
		// tkhd (version 0): 80-byte payload.
		{
			var b bytes.Buffer
			writeBoxHeader(&b, "tkhd", 72)
			b.Write([]byte{0, 0, 0x03, 0x00}) // version 0, flags 0x03 (enabled+in edit)
			b.Write(zero4)                    // creation_time
			b.Write(zero4)                    // modification_time
			b.Write(u32(1))                  // track_ID
			b.Write(zero4)                   // reserved
			b.Write(zero4)                   // duration (live)
			b.Write(make([]byte, 8))         // reserved
			b.Write([]byte{0, 0, 0, 1, 0, 0}) // layer, alternate_group, volume, reserved
			// matrix (identity).
			b.Write([]byte{
				0x00, 0x01, 0x00, 0x00, 0, 0, 0, 0,
				0, 0, 0, 0, 0x00, 0x01, 0x00, 0x00,
				0, 0, 0, 0, 0, 0, 0x40, 0x00,
			})
			b.Write(u32(uint32(m.width) << 16))  // width (fixed 16.16)
			b.Write(u32(uint32(m.height) << 16)) // height (fixed 16.16)
			trak.Write(b.Bytes())
		}
		// mdia: mdhd + hdlr + minf.
		var mdia bytes.Buffer
		{
			var b bytes.Buffer
			writeBoxHeader(&b, "mdhd", 20)
			b.Write([]byte{0, 0, 0, 0}) // version 0, flags 0
			b.Write(zero4)              // creation_time
			b.Write(zero4)              // modification_time
			b.Write(u32(1000000))      // timescale
			b.Write(zero4)              // duration (live)
			b.Write([]byte{0x55, 0xC4}) // language 'und'
			mdia.Write(b.Bytes())
		}
		{
			var b bytes.Buffer
			writeBoxHeader(&b, "hdlr", 21)
			b.Write([]byte{0, 0, 0, 0}) // version 0, flags 0
			b.Write(zero4)              // pre_defined
			b.Write([]byte{'v', 'i', 'd', 'e'}) // handler_type
			b.Write(make([]byte, 12))   // reserved
			b.Write([]byte("VideoHandler\000"))
			mdia.Write(b.Bytes())
		}
		var minf bytes.Buffer
		{
			var b bytes.Buffer
			writeBoxHeader(&b, "vmhd", 8)
			b.Write([]byte{0, 0, 0x01, 0x00}) // version 0, flags 0x01 (graphics enabled)
			b.Write([]byte{0, 0})             // opacity 1.0
			b.Write(make([]byte, 4))          // reserved
			minf.Write(b.Bytes())
		}
		{
			var dinf bytes.Buffer
			writeBoxHeader(&dinf, "dinf", 20)
			writeBoxHeader(&dinf, "dref", 12)
			binary.Write(&dinf, binary.BigEndian, uint32(1)) // entry count
			writeBoxHeader(&dinf, "url ", 4)
			dinf.Write([]byte{0, 0, 0x01, 0x00}) // self-contained (flags 0x01)
			minf.Write(dinf.Bytes())
		}
		{
			var stbl bytes.Buffer
			// stsd: one avc1 sample entry.
			{
				var avc1 bytes.Buffer
				avc1.Write(make([]byte, 12)) // reserved (data_reference_index at end)
				avc1.Write([]byte{0, 1})     // pre_defined + reserved
				avc1.Write(make([]byte, 2))  // reserved
				avc1.Write(make([]byte, 8))  // pre_defined
				binary.Write(&avc1, binary.BigEndian, uint16(m.width))
				binary.Write(&avc1, binary.BigEndian, uint16(m.height))
				avc1.Write(u32(0x00480000)) // horizresolution 72dpi
				avc1.Write(u32(0x00480000)) // vertresolution 72dpi
				avc1.Write(make([]byte, 4)) // reserved
				avc1.Write(u16(1))          // frame_count
				avc1.Write(make([]byte, 32)) // compressorname
				avc1.Write(u16(0x0018))     // depth 24
				avc1.Write(u16(0xFFFD))     // compressor ID (-1)
				// avcC.
				var avcC bytes.Buffer
				avcC.WriteByte(1) // configurationVersion
				avcC.Write(m.sps[5:8]) // profile/constraint/level
				avcC.WriteByte(0xFF)   // lengthSizeMinusOne = 3
				avcC.WriteByte(0xE0)   // numOfSequenceParameterSets = 1
				avcC.WriteByte(byte(len(m.sps) - 4))
				avcC.Write(m.sps[4:]) // SPS NAL unit (with header, without start code)
				avcC.WriteByte(1) // numOfPictureParameterSets
				avcC.WriteByte(byte(len(m.pps) - 4))
				avcC.Write(m.pps[4:]) // PPS NAL unit (with header, without start code)
				writeBoxHeader(&avc1, "avcC", len(avcC.Bytes()))
				avc1.Write(avcC.Bytes())
				avc1.Write(u16(1)) // data_reference_index
				var stsd bytes.Buffer
				writeBoxHeader(&stsd, "stsd", 8)
				binary.Write(&stsd, binary.BigEndian, uint32(1)) // entry count
				stsd.Write(avc1.Bytes())
				stbl.Write(stsd.Bytes())
			}
			// stts (empty: live stream; sample tables are per-fragment).
			{
				var b bytes.Buffer
				writeBoxHeader(&b, "stts", 4)
				binary.Write(&b, binary.BigEndian, uint32(0))
				stbl.Write(b.Bytes())
			}
			// stss (empty: the moof's trun carries per-fragment sync info).
			{
				var b bytes.Buffer
				writeBoxHeader(&b, "stss", 4)
				binary.Write(&b, binary.BigEndian, uint32(0))
				stbl.Write(b.Bytes())
			}
			// stsc (single run: every sample is in the current chunk).
			{
				var b bytes.Buffer
				writeBoxHeader(&b, "stsc", 12)
				binary.Write(&b, binary.BigEndian, uint32(1)) // entry count
				binary.Write(&b, binary.BigEndian, uint32(1)) // first_chunk
				binary.Write(&b, binary.BigEndian, uint32(1)) // samples_per_chunk
				binary.Write(&b, binary.BigEndian, uint32(1)) // sample_description_index
				stbl.Write(b.Bytes())
			}
			// stsz (zero: per-fragment).
			{
				var b bytes.Buffer
				writeBoxHeader(&b, "stsz", 8)
				binary.Write(&b, binary.BigEndian, uint32(0)) // sample_size
				binary.Write(&b, binary.BigEndian, uint32(0)) // sample_count
				stbl.Write(b.Bytes())
			}
			// stco (zero: per-fragment).
			{
				var b bytes.Buffer
				writeBoxHeader(&b, "stco", 4)
				binary.Write(&b, binary.BigEndian, uint32(0))
				stbl.Write(b.Bytes())
			}
			minf.Write(stbl.Bytes())
		}
		mdia.Write(minf.Bytes())
		trak.Write(mdia.Bytes())
		patchBoxSize(&trak, uint32(trak.Len()))
		moov.Write(trak.Bytes())
	}
	// mvex: the MPEG-4 fragment (mvex) box, required for fragmented MP4 (and
	// for Chrome's MSE). It carries a single trex (track extended header) for
	// track 1. The trex defaults tell the parser the per-fragment sample
	// description index and that the per-sample duration/size/flags are
	// carried by the trun (all zero here - the trun's flags 0x00F marks them
	// present per-sample). Without mvex/trex, Chrome's segment parser rejects
	// the moov ("stream parsing failed").
	//
	// trex (version 0): header (8) + version/flags (4) + track_ID (4) +
	// default-sample-description-index (4) + default-sample-duration (4) +
	// default-sample-size (4) + default-sample-flags (4) = 32 bytes.
	// mvex: header (8) + trex (32) = 40 bytes.
	{
		var mvex bytes.Buffer
		writeBoxHeader(&mvex, "mvex", 32)
		var b bytes.Buffer
		writeBoxHeader(&b, "trex", 24)
		b.Write([]byte{0, 0, 0, 0}) // version 0, flags 0
		b.Write(u32(1))             // track_ID
		b.Write(u32(1))             // default-sample-description-index
		b.Write(u32(0))             // default-sample-duration
		b.Write(u32(0))             // default-sample-size
		b.Write(u32(0))             // default-sample-flags
		mvex.Write(b.Bytes())
		moov.Write(mvex.Bytes())
	}
	patchBoxSize(&moov, uint32(moov.Len()))
	out.Write(moov.Bytes())
	return out.Bytes()
}

// buildMoof builds the moof box for the pending fragment: a single traf
// (tfhd + tfdt + trun) describing the fragment's samples. The tfdt base
// time is 0 (the client plays in sequence mode, which ignores it); the
// trun carries per-sample durations (constant CFR), sizes, and flags, plus
// a data-offset pointing into the mdat payload.
func (m *fmp4Muxer) buildMoof() []byte {
	n := len(m.sampleSizes)
	frameDur := m.frameDur()
	// trun size: header (8) + version/flags (4) + sample_count (4) +
	// data_offset (4) + per-sample (duration 4 + size 4 + flags 4) * n.
	trunSize := 20 + 12*n
	// tfhd box total: header (8) + version/flags (4) + track_ID (4) = 16.
	// tfdt box total: header (8) + version/flags (4) + base_time (4) = 16.
	// traf size: header (8) + tfhd (16) + tfdt (16) + trun.
	trafSize := 8 + 16 + 16 + trunSize
	// moof size: header (8) + mfhd (12) + traf.
	moofSize := 8 + 12 + trafSize
	// data-offset: relative to the start of the trun box. The trun box
	// starts at offset 60 within the moof (moof header 8 + mfhd 12 + traf
	// header 8 + tfhd 16 + tfdt 16); the mdat payload starts at
	// moofSize + 8 (after the moof and the 8-byte mdat header).
	dataOffset := uint32(moofSize + 8 - 60)

	var moof bytes.Buffer
	writeBoxHeader(&moof, "moof", moofSize-8)
	// mfhd: sequence_number (monotonic fragment counter; wraps are fine).
	{
		var b bytes.Buffer
		writeBoxHeader(&b, "mfhd", 4)
		b.Write(u32(m.fragStart>>0)) // sequence_number (track-time derived)
		moof.Write(b.Bytes())
	}
	var traf bytes.Buffer
	writeBoxHeader(&traf, "traf", trafSize-8)
	// tfhd: version/flags word (0x00000001 = default-base-is-moof) so no
	// default-base-offset follows; the tfdt carries the fragment base time.
	// The box is 16 bytes total: header (8) + version/flags (4) + track_ID (4),
	// matching the trafSize math above (tfhd = 16).
	{
		var b bytes.Buffer
		writeBoxHeader(&b, "tfhd", 8)
		b.Write([]byte{0, 0, 0x00, 0x01}) // version 0, flags 0x01 (default-base-is-moof)
		b.Write(u32(1))                   // track_ID
		traf.Write(b.Bytes())
	}
	// tfdt (version 0): base time 0 (sequence mode ignores it). The box is
	// 16 bytes total: header (8) + version/flags (4) + base_time (4),
	// matching the trafSize math above (tfdt = 16).
	{
		var b bytes.Buffer
		writeBoxHeader(&b, "tfdt", 8)
		b.Write([]byte{0, 0, 0, 0}) // version 0, flags 0
		b.Write(u32(0))             // base_time
		traf.Write(b.Bytes())
	}
	// trun: flags 0x00F = data-offset + sample-duration + sample-size +
	// sample-flags, all present.
	{
		var b bytes.Buffer
		writeBoxHeader(&b, "trun", trunSize-8)
		b.Write([]byte{0, 0, 0x0F}) // version 0, flags 0x00F
		b.Write(u32(uint32(n)))    // sample_count
		b.Write(u32(dataOffset))    // data_offset
		for i := 0; i < n; i++ {
			b.Write(u32(frameDur)) // sample_duration (CFR)
			b.Write(u32(m.sampleSizes[i]))
			// sample_flags: depends-on / redundant / uses-redundant /
			// padding / is-non-sync / degradation / encoding / oops.
			var flags uint32
			if i == 0 && m.fragIDR {
				// First sample of a keyframe-aligned fragment: a sync sample
				// (is_non_sync = 0), no redundancy.
				flags = 0x01010101
			} else {
				// Non-sync sample (is_non_sync = 1), no redundancy.
				flags = 0x01010101 | 0x00020000
			}
			b.Write(u32(flags))
		}
		traf.Write(b.Bytes())
	}
	moof.Write(traf.Bytes())
	return moof.Bytes()
}

// zero4 is a 4-byte zero value (creation/modification times).
var zero4 = []byte{0, 0, 0, 0}

// u32 returns a 4-byte big-endian value.
func u32(v uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return b[:]
}

// u16 returns a 2-byte big-endian value.
func u16(v uint16) []byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	return b[:]
}

// writeBoxHeader writes an 8-byte box header (size = 8 + payloadSize + type).
func writeBoxHeader(b *bytes.Buffer, typ string, payloadSize int) {
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(8+payloadSize))
	copy(hdr[4:8], typ)
	b.Write(hdr[:])
}

// patchBoxSize rewrites the size field (bytes 0-3) of a box whose header is
// at the start of b to the box's actual total size (header + payload).
func patchBoxSize(b *bytes.Buffer, totalSize uint32) {
	binary.BigEndian.PutUint32(b.Bytes()[0:4], totalSize)
}

// parseSPSDimensions extracts the width and height from an SPS NAL unit
// (with a 4-byte start code prefix) by parsing the exp-golomb coded fields
// up to pic_width_in_mbs_minus1 and pic_height_in_map_units_minus1. Returns
// (0, 0) if the SPS is malformed.
func parseSPSDimensions(sps []byte) (int, int) {
	// sps[0:4] is the start code; sps[4] is the NAL header; the RBSP starts
	// at sps[5] (no emulation prevention byte removal needed for the fields
	// we parse - they are short and do not hit the 00 00 01 pattern in
	// practice for the header fields; a full parser would remove them).
	if len(sps) < 8 {
		return 0, 0
	}
	rbsp := sps[5:]
	bit := 0
	getBit := func() int {
		if bit >= len(rbsp)*8 {
			return 0
		}
		v := (rbsp[bit/8] >> (7 - bit%8)) & 1
		bit++
		return int(v)
	}
	skip := func(n int) {
		for i := 0; i < n; i++ {
			getBit()
		}
	}
	u := func() int {
		v := 0
		for getBit() == 1 {
			v = (v << 1) + 1
		}
		return v
	}
	signed := func() int {
		v := u()
		if v&1 != 0 {
			return -(v >> 1)
		}
		return v >> 1
	}
	_ = u() // profile_idc
	skip(8) // constraint flags + reserved
	_ = u() // level_idc
	_ = u() // seq_parameter_set_id
	cfi := u() // chroma_format_idc
	if cfi == 3 {
		_ = u() // separate_colour_plane_flag
	}
	_ = u() // log2_max_frame_num_minus4
	poc := u() // pic_order_cnt_type
	if poc == 0 {
		_ = u() // log2_max_pic_order_cnt_l0_minus4
		_ = u()
	} else if poc == 1 {
		skip(1)
		_ = signed()
		_ = signed()
		skip(5)
	}
	_ = u() // num_ref_frames
	skip(1) // gaps_in_frame_num_value_allowed_flag
	widthMbs := u() + 1 // pic_width_in_mbs_minus1
	heightMapUnits := u() + 1 // pic_height_in_map_units_minus1
	width := widthMbs * 16
	height := heightMapUnits * 16
	// (Subsequent SPS fields are not needed for the tkhd dimensions.)
	return width, height
}

// containsNalType reports whether the Annex-B access unit buf contains a NAL
// unit of the given type (low 5 bits of the first NAL byte).
func containsNalType(buf []byte, typ int) bool {
	for {
		sc := findStartCode(buf)
		if sc < 0 {
			return false
		}
		nal := buf[sc:]
		if len(nal) >= 4 && nal[0] == 0 && nal[1] == 0 && nal[2] == 0 && nal[3] == 1 {
			nal = nal[4:]
		} else if len(nal) >= 3 && nal[0] == 0 && nal[1] == 0 && nal[2] == 1 {
			nal = nal[3:]
		}
		if len(nal) == 0 {
			return false
		}
		if int(nal[0]&0x1F) == typ {
			return true
		}
		// Advance past this NAL unit to the next start code.
		next := findStartCodeAfter(nal, 0)
		if next < 0 {
			return false
		}
		buf = nal[next:]
	}
}

// findStartCodeAfter returns the offset of the first start code in buf that
// is at or after the given offset, or -1 if none.
func findStartCodeAfter(buf []byte, offset int) int {
	if offset >= len(buf) {
		return -1
	}
	return findStartCode(buf[offset:]) + offset
}
