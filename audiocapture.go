package main

import (
	"io"
	"log"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// AudioCapture owns the audio GStreamer pipeline that captures the system
// audio (PulseAudio/PipeWire monitor source) and encodes it to opus. The
// pipeline emits an Ogg-muxed opus stream on stdout; the Go process is the
// single consumer of that stream.
//
// The pipeline is:
//
//	gst-launch-1.0 -q pulsesrc [device=$SRC] ! audioconvert ! audioresample
//	  ! audio/x-raw,rate=48000 ! opusenc bitrate=32000 ! oggmux ! fdsink fd=1
//
// The fdsink runs with its default sync=true: the sink paces its writes to
// the pipeline clock, which backpressures the oggmux so it emits Ogg pages
// in real time. (An earlier "burst-then-halt" symptom was NOT caused by the
// fdsink sync mode — the pipeline read continuously; it was a lacing-value
// desync in the oggDemuxer, see the lacing note in nextPage.)
//
// where $SRC is the monitor source (a null-sink monitor that taps the system
// audio). The oggmux element frames the opus packets into Ogg pages: the raw
// opus byte stream is NOT length-delimited (opus packets are variable-length
// and not self-delimiting), so the client cannot split it into packets
// without the Ogg page framing. The Go pump (run) reads the Ogg stream,
// demuxes it into individual opus packets (oggDemuxer), and fans each packet
// out to the /audio WebSocket subscribers (rs.audio).
//
// The monitor source taps the output of a sink (the system audio the user
// hears). The default (COAXIAL_AUDIO_SOURCE unset) is
// "@DEFAULT_MONITOR@" - PulseAudio's special name for the default sink's
// monitor, i.e. the system audio the user hears. This is critical: a bare
// pulsesrc with NO device property captures the default SOURCE (the
// microphone), which is silent when the mic is unused and makes opus emit
// ~2-4 byte silence packets. A specific sink can be named to tap only that
// sink's output.
type AudioCapture struct {
	src string // pulsesrc device (monitor source); "" = default monitor

	// gst holds the audio GStreamer pipeline (pulsesrc -> opusenc -> oggmux),
	// terminated on teardown. pgid is the process-group id the pipeline runs
	// in (Setpgid) so teardown can signal the whole group.
	gst  *exec.Cmd
	pgid int

	// teardownOnce ensures the pipeline is stopped exactly once.
	teardownOnce sync.Once
}

// newAudioCapture builds an AudioCapture from the environment. The
// COAXIAL_AUDIO_SOURCE env var names the pulsesrc monitor device to tap.
// Unset, it defaults to "@DEFAULT_MONITOR@" (the default sink's monitor =
// the system audio the user hears). A bare pulsesrc with no device property
// would instead capture the default SOURCE (the microphone), which is wrong
// for a screen relay: a silent mic makes opus emit ~2-4 byte silence
// packets, which is exactly the "tiny packet" symptom this default avoids.
func newAudioCapture() *AudioCapture {
	src := os.Getenv("COAXIAL_AUDIO_SOURCE")
	if src == "" {
		src = "@DEFAULT_MONITOR@"
	}
	log.Printf("audio: monitor source %q (COAXIAL_AUDIO_SOURCE)", src)
	return &AudioCapture{src: src}
}

// run spawns the audio GStreamer pipeline and pumps the live Ogg-muxed opus
// byte stream until the pipeline dies. It is called in a goroutine by
// ensureAudio. It returns false if it failed to start the pipeline.
//
// The pump reads stdout in chunks, feeds each chunk to a stateful Ogg
// demuxer, and fans each complete opus packet out to rs.audio (one binary WS
// message per packet). A slow client's channel fills and the lag-drop policy
// drops it, so the pump never blocks on a client.
func (c *AudioCapture) run(rs *RelayServer) bool {
	args := []string{
		"gst-launch-1.0", "-q",
		"pulsesrc", 
	}
	if c.src != "" {
		args = append(args, "device="+c.src)
	}
	args = append(args,
		"!", "audioconvert",
		"!", "audioresample",
		"!", "audio/x-raw,rate=48000,channels=2",
		"!", "opusenc", "bitrate=32000",
		"!", "oggmux",
		// fdsink with default sync=true: paces writes to the pipeline clock so
		// the oggmux emits pages in real time.
		"!", "fdsink", "fd=1",
	)
	gstCmd := exec.Command(args[0], args[1:]...)
	gstCmd.Stderr = os.Stderr

	// Attach the pipeline's stdout (Ogg-muxed opus byte stream) before
	// starting.
	stdout, err := gstCmd.StdoutPipe()
	if err != nil {
		log.Printf("audio: failed to attach gst-launch stdout: %v", err)
		c.teardown()
		return false
	}

	// Run the pipeline in its own process group so teardown can signal the
	// whole group (the pipeline spawns child processes that a single-process
	// signal would leave orphaned).
	gstCmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := gstCmd.Start(); err != nil {
		log.Printf("audio: failed to start gst-launch: %v", err)
		c.teardown()
		return false
	}
	c.gst = gstCmd
	c.pgid = gstCmd.Process.Pid
	log.Printf("audio: gst-launch (pid %d, pgid %d) started", gstCmd.Process.Pid, c.pgid)

	// Optional: dump the raw Ogg stream to a file for inspection (gated by
	// COAXIAL_AUDIO_DUMP, e.g. "audio_dump.ogg"). This lets you run
	// `ffmpeg -i audio_dump.ogg -f s16le out.pcm` or `ogginfo audio_dump.ogg`
	// to see exactly what the pipeline is producing.
	var dumpW io.WriteCloser
	if dumpPath := os.Getenv("COAXIAL_AUDIO_DUMP"); dumpPath != "" {
		f, err := os.Create(dumpPath)
		if err != nil {
			log.Printf("audio: failed to open dump file %q: %v", dumpPath, err)
		} else {
			dumpW = f
			log.Printf("audio: dumping raw Ogg stream to %q", dumpPath)
		}
	}
	defer func() {
		if dumpW != nil {
			dumpW.Close()
		}
	}()

	// Pump the live Ogg-muxed opus byte stream until the pipeline exits (EOF)
	// or is torn down. The stream is a continuous Ogg page stream; we read it
	// in chunks, feed each chunk to the stateful demuxer, and fan each
	// complete opus packet out to rs.audio.
	//
	demux := newOggDemuxer(
		func(pkt []byte) {
			// Copy the packet: the demuxer's onPkt callback receives a slice of
			// its internal buffer (d.buf / d.pending), which is REUSED on the
			// next feed (append may grow/shift the backing array). rs.audio.write
			// fans the slice out to client channels that the WS write-pump
			// goroutines read ASYNCHRONOUSLY - by the time a client reads it, the
			// demuxer may have appended new data into the same backing array,
			// corrupting the packet (the client then receives a mangled opus
			// packet and its AudioDecoder throws / closes). Copying here
			// (synchronously, before feed returns and reuses the buffer) gives
			// each client an independent, stable copy.
			pktCopy := make([]byte, len(pkt))
			copy(pktCopy, pkt)
			rs.audio.write(pktCopy)
		},
		func(head []byte) {
			// The OpusHead carries the stream's channel count and sample rate.
			// Copy it (same buffer-reuse reason as above) and hand it to the
			// broadcaster, which caches it and re-emits it to every new
			// subscriber (like the fMP4 init segment) so the client can
			// configure its AudioDecoder to match the ACTUAL stream rather than
			// a hard-coded 48kHz/2ch guess.
			headCopy := make([]byte, len(head))
			copy(headCopy, head)
			rs.audio.setHead(headCopy)
		},
	)
	const readBufferSize = 64 * 1024
	buf := make([]byte, readBufferSize)
	for {
		n, err := stdout.Read(buf)
		if n > 0 {
			if dumpW != nil {
				dumpW.Write(buf[:n])
			}
			demux.feed(buf[:n])
		}
		if err != nil {
			// EOF or read error: the pipeline exited.
			break
		}
	}
	// Flush any partial page/packet held in the demuxer (best-effort; a
	// torn-down pipeline may leave a trailing incomplete page).
	demux.flush()

	// Wait for the pipeline to exit.
	if err := gstCmd.Wait(); err != nil {
		log.Printf("audio: gst-launch exited: %v", err)
	} else {
		log.Printf("audio: gst-launch exited cleanly")
	}
	return true
}

// teardown terminates the audio GStreamer pipeline subprocess. Safe to call
// more than once.
func (c *AudioCapture) teardown() {
	c.teardownOnce.Do(func() {
		// Signal the whole pipeline process group (Setpgid), not just the
		// gst-launch process: the pipeline spawns child processes (PulseAudio
		// source, opus encoder) that a single-process signal would leave
		// orphaned.
		if c.pgid != 0 {
			_ = syscall.Kill(-c.pgid, syscall.SIGINT)
			// Guarantee: if the group ignores SIGINT (and the pump goroutine
			// stays blocked on its stdout read), force-kill it after a short
			// grace period. Kill is a no-op if the group already exited.
			go func(pgid int) {
				time.Sleep(2 * time.Second)
				_ = syscall.Kill(-pgid, syscall.SIGKILL)
			}(c.pgid)
		} else if c.gst != nil && c.gst.Process != nil {
			_ = c.gst.Process.Signal(os.Interrupt)
		}
	})
}

// oggDemuxer splits a continuous Ogg page byte stream into opus packets.
//
// An Ogg page is:
//
//	"OggS" (4) | version (1) | header-type (1) | granule (8) | serial (4) |
//	sequence (4) | crc (4) | n-segments (1) | segment-table (n) | payload
//
// The segment table is a run of lacing values. oggmux (GStreamer) writes the
// lacing value as the segment SIZE directly: a value < 255 is the exact
// segment length and terminates the packet; a value of 255 means the
// segment is 255 bytes and the packet continues in the next segment. (This
// differs from the RFC 3533 convention, where a value < 255 means the
// segment is value+1 bytes. Verified empirically against oggmux output: the
// 19-byte OpusHead packet has lacing 19, and the Ogg page CRC verifies at
// the lacing-value-as-size boundary. Using value+1 over-counts by one byte
// per segment, desyncing the demuxer.) A packet is a run of segments ending
// in a value < 255. A packet may span multiple segments (within a page) or
// multiple pages (the header-type continuation bit, 0x01, marks a page
// whose first packet is a continuation of the previous page's last packet).
//
// For opus, the first two packets are the OpusHead and OpusTags headers
// (identified by their magic prefixes); the demuxer skips them and emits
// only the audio packets.
type oggDemuxer struct {
	buf     []byte // residual bytes not yet consumed as complete pages
	pending []byte // partial packet (continuation from the previous page)
	onPkt   func([]byte) // audio packet (headers excluded)
	onHead  func([]byte) // the OpusHead packet, emitted once (may be nil)
}

// newOggDemuxer creates a demuxer that emits each complete opus audio packet
// via onPkt and the OpusHead packet (once) via onHead. onPkt/onHead receive
// a slice of the demuxer's internal buffer (NOT a copy): the caller must
// consume or copy them before the next feed call. OpusTags is always dropped.
func newOggDemuxer(onPkt func([]byte), onHead func([]byte)) *oggDemuxer {
	return &oggDemuxer{onPkt: onPkt, onHead: onHead}
}

// feed appends a chunk to the residual and emits every complete opus packet
// it now contains via onPkt.
func (d *oggDemuxer) feed(chunk []byte) {
	d.buf = append(d.buf, chunk...)
	for {
		page, rest, ok := d.nextPage()
		if !ok {
			// Incomplete page (rest == buf: wait for more data) or resync
			// (rest == buf[1:]: a non-magic byte was skipped). Either way the
			// residual advances to rest; only decode when a complete page was
			// found.
			d.buf = rest
			return
		}
		d.buf = rest
		d.decodePage(page)
	}
}

// flush emits any partial packet held in pending (best-effort; a torn-down
// pipeline may leave a trailing incomplete page, which is dropped).
func (d *oggDemuxer) flush() {
	// A partial packet (pending) is incomplete (its page was never
	// completed); drop it. The residual (buf) may hold a partial page; drop
	// it too.
	d.pending = nil
	d.buf = nil
}

// nextPage extracts the next complete Ogg page from the residual, returning
// the page bytes (header + segment table + payload) and the remaining
// residual. ok is false if the residual does not yet hold a complete page
// (the page header or payload spans into the next chunk).
func (d *oggDemuxer) nextPage() (page []byte, rest []byte, ok bool) {
	buf := d.buf
	// Need at least the fixed 27-byte header to read the segment count.
	if len(buf) < 27 {
		return nil, buf, false
	}
	if buf[0] != 'O' || buf[1] != 'g' || buf[2] != 'g' || buf[3] != 'S' {
		// Not an Ogg page start (the stream is misaligned or corrupt). Skip
		// one byte and resync (best-effort; a real stream is always aligned).
		return nil, buf[1:], false
	}
	nSeg := int(buf[26])
	// The page is 27 (header) + nSeg (segment table) + payload bytes.
	segTableEnd := 27 + nSeg
	if len(buf) < segTableEnd {
		return nil, buf, false
	}
	// Sum the lacing values to get the payload length.
	//
	// NOTE: oggmux (GStreamer) writes the lacing value as the segment SIZE
	// directly (a value < 255 is the exact segment length; 255 means the
	// segment continues in the next segment). This differs from the RFC 3533
	// convention (value < 255 => size = value+1). Verified empirically: the
	// OpusHead packet is exactly 19 bytes and its lacing value is 19; the
	// OpusTags payload is exactly 47 bytes and its lacing value is 47; and
	// the Ogg page CRC (RFC 3533, computed over the page with the CRC field
	// zeroed) verifies at the lacing-value-as-size boundary for every page.
	// Using value+1 (the RFC convention) over-counts by 1 per segment, which
	// desyncs the demuxer by one byte per page and starves the stream.
	payloadLen := 0
	for i := 0; i < nSeg; i++ {
		payloadLen += int(buf[27+i])
	}
	pageEnd := segTableEnd + payloadLen
	if len(buf) < pageEnd {
		return nil, buf, false
	}
	// Sanity-check the page is plausible before trusting it. A real Ogg page
	// has a small segment table (each page holds at most a few opus packets,
	// each spanning a handful of 255-byte segments) and a payload of a
	// reasonable size. If the segment count or payload length is absurd, the
	// "OggS" we matched is a false positive (random payload bytes that happen
	// to spell OggS) and the stream is misaligned. Reject the page and resync
	// from the next byte rather than emitting a garbage fragment.
	if nSeg > 255 || payloadLen > 65536 {
		return nil, buf[1:], false
	}
	return buf[:pageEnd], buf[pageEnd:], true
}

// decodePage parses a single complete Ogg page and emits the opus packets it
// contains (skipping the OpusHead/OpusTags headers). A packet that continues
// onto the next page is held in d.pending.
func (d *oggDemuxer) decodePage(page []byte) {
	nSeg := int(page[26])
	segTable := page[27 : 27+nSeg]
	payload := page[27+nSeg:]

	// Walk the segment table, accumulating packet data. A lacing value < 255
	// terminates the current packet; 255 continues it in the next segment.
	offset := 0
	for i := 0; i < nSeg; i++ {
		// Lacing value = segment size directly (oggmux convention; 255
		// means the segment continues in the next one). See nextPage.
		size := int(segTable[i])
		if offset+size > len(payload) {
			// Malformed page (payload shorter than the segment table
			// implies); stop here (best-effort).
			return
		}
		d.pending = append(d.pending, payload[offset:offset+size]...)
		offset += size
		if size != 255 {
			// Packet complete. Emit it (unless it is a header).
			pkt := d.pending
			d.pending = nil
			d.emit(pkt)
		}
	}
	// If the page's header-type has the continuation bit (0x01) set, the
	// page's FIRST packet is a continuation of the previous page's last
	// packet. But we've already accumulated it into d.pending (or emitted it
	// if it completed on this page). The continuation bit tells us the
	// packet at the START of this page is not a fresh packet. Since we
	// process pages in order and hold partial packets in d.pending across
	// pages, the continuation is handled naturally: a packet that did not
	// complete on the previous page is still in d.pending and will be
	// completed by a segment on this page. The header-type bit is therefore
	// informational (we rely on the lacing values, not the bit, to track
	// packet boundaries).
}

// emit routes a packet: OpusHead (identified by its magic prefix) goes to
// onHead (once; the client needs it to configure its decoder with the
// stream's actual channel count and sample rate), OpusTags is dropped, and
// everything else is an audio packet emitted via onPkt.
func (d *oggDemuxer) emit(pkt []byte) {
	if len(pkt) >= 8 {
		if string(pkt[:8]) == "OpusHead" {
			if d.onHead != nil {
				d.onHead(pkt)
			}
			return
		}
		if string(pkt[:8]) == "OpusTags" {
			return
		}
	}
	d.onPkt(pkt)
}

// oggPageHeader is the fixed Ogg page header (27 bytes) + segment table.
// Exposed for tests.
const oggPageHeaderSize = 27

// oggPageEnd computes the total length of an Ogg page given its header +
// segment table (page[:27+nSeg]). Returns -1 if the page is malformed.
func oggPageEnd(page []byte) int {
	if len(page) < oggPageHeaderSize {
		return -1
	}
	nSeg := int(page[26])
	if len(page) < oggPageHeaderSize+nSeg {
		return -1
	}
	payloadLen := 0
	for i := 0; i < nSeg; i++ {
		payloadLen += int(page[27+i])
	}
	return oggPageHeaderSize + nSeg + payloadLen
}

// buildOggPage assembles a single Ogg page from the given packets (used by
// tests). It uses a fixed serial number and the provided page sequence
// number. The granule position is set to the provided sample index (48kHz).
// The CRC is zeroed (the demuxer does not verify it; a real encoder computes
// it, but for tests a zero CRC is sufficient).
//
// Each packet is split into 255-byte segments (lacing value 255) with a
// final segment of size 1-255 (lacing value = size). A lacing value of 255
// means the segment is 255 bytes and the packet continues in the next
// segment; a value < 255 means the segment is exactly that many bytes and
// the packet ends there. (This matches oggmux's convention, where the lacing
// value is the segment size directly, not size-1 as in RFC 3533.)
func buildOggPage(packets [][]byte, granule int64, seq uint32) []byte {
	var lacing []byte
	var payload []byte
	for _, pkt := range packets {
		off := 0
		for off < len(pkt) {
			chunk := len(pkt) - off
			if chunk > 255 {
				chunk = 255
			}
			if chunk == 255 && off+chunk < len(pkt) {
				// A full 255-byte segment that is NOT the last: lacing 255.
				lacing = append(lacing, 255)
			} else {
				// The final segment of this packet: lacing = chunk (size).
				lacing = append(lacing, byte(chunk))
			}
			payload = append(payload, pkt[off:off+chunk]...)
			off += chunk
		}
	}
	page := make([]byte, 0, 27+len(lacing)+len(payload))
	page = append(page, 'O', 'g', 'g', 'S')
	page = append(page, 0) // version
	page = append(page, 0) // header-type (no continuation, no flags)
	page = append(page, le64(granule)...)
	page = append(page, le32(1)...) // serial number
	page = append(page, le32(seq)...)
	page = append(page, 0, 0, 0, 0) // crc (zeroed)
	page = append(page, byte(len(lacing)))
	page = append(page, lacing...)
	page = append(page, payload...)
	return page
}

// le64 encodes v as a little-endian 8-byte integer.
func le64(v int64) []byte {
	b := make([]byte, 8)
	for i := 0; i < 8; i++ {
		b[i] = byte(v >> (8 * i))
	}
	return b
}

// le32 encodes v as a little-endian 4-byte integer.
func le32(v uint32) []byte {
	return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)}
}
