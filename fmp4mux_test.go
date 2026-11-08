package main

// Phase 0 tests: fMP4 late-joiner init re-emit, monotonic mfhd fragment
// counter, and moof/mfhd structural validity.

import (
	"bytes"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// bitWriter accumulates bits MSB-first into a byte slice, growing the slice
// as needed (a bit cursor is tracked explicitly; the slice length alone does
// not determine the write position).
type bitWriter struct {
	buf  []byte
	nbit int // bits written so far
}

func (w *bitWriter) bit(b int) {
	pos := w.nbit % 8
	if pos == 0 {
		w.buf = append(w.buf, 0)
	}
	if b == 1 {
		w.buf[len(w.buf)-1] |= 0x80 >> pos
	}
	w.nbit++
}

func (w *bitWriter) bits(v, n int) {
	for i := n - 1; i >= 0; i-- {
		w.bit((v >> i) & 1)
	}
}

// expGolombBits appends v as an H.264 exp-golomb (ue) code: (k-1) one-bits
// followed by v+1 in k bits (most significant bit first), where k is the
// number of bits in v+1. Example: v=0 -> "1", v=1 -> "110", v=3 -> "11100".
func (w *bitWriter) expGolomb(v int) {
	code := v + 1
	k := 0
	for c := code; c > 0; c >>= 1 {
		k++
	}
	for i := 0; i < k-1; i++ {
		w.bit(1)
	}
	w.bits(code, k)
}

// testSPS builds a minimal SPS NAL (with 4-byte start code) whose RBSP fields
// up to pic_width_in_mbs_minus1 / pic_height_in_map_units_minus1 are zero
// except the two size fields. parseSPSDimensions reads exactly that prefix, so
// it returns (widthMbs*16, heightMbs*16).
func testSPS(widthMbs, heightMbs int) []byte {
	var w bitWriter
	w.expGolomb(0) // profile_idc
	w.bits(0, 8)   // constraint flags + reserved
	w.expGolomb(0) // level_idc
	w.expGolomb(0) // seq_parameter_set_id
	w.expGolomb(0) // chroma_format_idc (0 = 4:2:0)
	w.expGolomb(0) // log2_max_frame_num_minus4
	w.bit(0)      // pic_order_cnt_type (0)
	w.expGolomb(0) // log2_max_pic_order_cnt_l0_minus4
	w.expGolomb(0) // num_ref_frames
	w.bit(0)      // gaps_in_frame_num_value_allowed_flag
	w.expGolomb(widthMbs - 1)
	w.expGolomb(heightMbs - 1)
	// NAL header 0x47 (SPS), then the RBSP.
	return append([]byte{0, 0, 0, 1, 0x47}, w.buf...)
}

// testPPS builds a minimal PPS NAL (with 4-byte start code).
func testPPS() []byte {
	// RBSP: pic_parameter_set_id (eg0), seq_parameter_set_id (eg0),
	// entropy_coding_mode_flag (0), num_slice_groups_idc (eg0),
	// num_ref_idx_l0/l1_default_active_minus1 (eg0 each),
	// ref_pic_list_modification_flag_l0/l1 (0 each), num_ref_frames (eg0),
	// weighted_pred (0), weighted_b_pred (0), then 14 zero offset bits.
	// 34 bits total -> padded to 40 (5 bytes).
	rbsp := []byte{0x00, 0x00, 0x00, 0x00, 0x00}
	return append([]byte{0, 0, 0, 1, 0x48}, rbsp...)
}

// testIDR builds an access unit: SPS + PPS + IDR slice NAL.
func testIDR() []byte {
	var au []byte
	au = append(au, testSPS(64, 36)...)
	au = append(au, testPPS()...)
	au = append(au, 0, 0, 0, 1, 0x65, 0xFF, 0xFF, 0xFF) // IDR slice (type 5)
	return au
}

// testNonIDR builds a non-IDR slice access unit (no SPS/PPS).
func testNonIDR() []byte {
	return []byte{0, 0, 0, 1, 0x41, 0xAB, 0xCD} // slice type 1
}

// TestFmp4InitSegment verifies the init segment is built once (ftyp+moov,
// with avcC) and that the first fragment carries a moof.
func TestFmp4InitSegment(t *testing.T) {
	var inits, frags [][]byte
	mux := newFmp4Muxer(30, func(b []byte) {
		frags = append(frags, append([]byte{}, b...))
	}, func(b []byte) {
		inits = append(inits, append([]byte{}, b...))
	})
	mux.noteSPSPPS(testIDR())
	mux.bumpFrame()
	mux.feed(testIDR())
	if len(inits) != 1 {
		t.Fatalf("init emitted %d times, want 1", len(inits))
	}
	init := inits[0]
	// The init segment begins with the ftyp box: size(4) + brand "isom"(4) +
	// minor version(4) + compatible brands. (The ftyp box's TYPE field is the
	// major brand "isom", not the literal string "ftyp".)
	if len(init) < 8 || string(init[4:8]) != "isom" {
		t.Fatalf("init does not start with the ftyp box (brand isom): %x", init[:8])
	}
	if !bytes.Contains(init, []byte("moov")) {
		t.Fatal("init does not contain moov")
	}
	if !bytes.Contains(init, []byte("avcC")) {
		t.Fatal("init does not contain avcC")
	}
	if len(frags) != 1 {
		t.Fatalf("fragments = %d, want 1 (IDR closes the fragment)", len(frags))
	}
	if !bytes.Contains(frags[0], []byte("moof")) {
		t.Fatal("fragment does not contain moof")
	}
	// A second IDR must NOT re-emit the init.
	mux.bumpFrame()
	mux.feed(testIDR())
	if len(inits) != 1 {
		t.Fatalf("init re-emitted: %d, want 1", len(inits))
	}
}

// parseMoof parses the moof box in frag and returns (sequence, sampleCount,
// dataOffset). It validates the box sizes so a size/offset regression is
// caught (the mfhd box must be 16 bytes: header + version/flags +
// sequence_number).
func parseMoof(t *testing.T, frag []byte) (seq, sampleCount, dataOffset uint32) {
	t.Helper()
	typeIdx := bytes.Index(frag, []byte("moof"))
	if typeIdx < 4 {
		t.Fatalf("no moof box in fragment (index %d)", typeIdx)
	}
	// The moof box STARTS at typeIdx-4 (the 4-byte size field precedes the
	// "moof" type). All offsets below are relative to the box start.
	start := typeIdx - 4
	moofSize := binary.BigEndian.Uint32(frag[start : start+4])
	if start+int(moofSize) > len(frag) {
		t.Fatalf("moof size %d exceeds fragment %d", moofSize, len(frag))
	}
	// moof payload: mfhd (16) + traf (...).
	if len(frag) < start+8+16 {
		t.Fatalf("moof too small: %d", len(frag)-start)
	}
	// mfhd box at start+8: size(4) type(4) version/flags(4) sequence(4).
	if string(frag[start+12:start+16]) != "mfhd" {
		t.Fatalf("moof payload does not start with mfhd: %x", frag[start+12:start+16])
	}
	mfhdSize := binary.BigEndian.Uint32(frag[start+8 : start+12])
	if mfhdSize != 16 {
		t.Fatalf("mfhd size = %d, want 16 (header 8 + version/flags 4 + sequence 4)", mfhdSize)
	}
	// mfhd sequence_number (the monotonic fragment counter).
	seq = binary.BigEndian.Uint32(frag[start+20 : start+24])
	// traf box at start+24: type at start+28.
	if string(frag[start+28:start+32]) != "traf" {
		t.Fatalf("box after mfhd is %q, want traf", frag[start+28:start+32])
	}
	// traf payload: tfhd (16) + tfdt (16) + trun (...). The trun box starts at
	// start+64 (mfhd 16 + traf header 8 + tfhd 16 + tfdt 16).
	trunStart := start + 64
	if len(frag) < trunStart+8 {
		t.Fatal("no trun in traf")
	}
	if string(frag[trunStart+4:trunStart+8]) != "trun" {
		t.Fatalf("box after tfdt is %q, want trun", frag[trunStart+4:trunStart+8])
	}
	// trun payload: version/flags (4) + sample_count (4) + data_offset (4) +
	// per-sample (12 each).
	sampleCount = binary.BigEndian.Uint32(frag[trunStart+12 : trunStart+16])
	dataOffset = binary.BigEndian.Uint32(frag[trunStart+16 : trunStart+20])
	// data_offset is relative to the trun box start and must point at the
	// mdat payload: moofEnd + 8 (the mdat header).
	moofEnd := start + int(moofSize)
	if int(dataOffset) != moofEnd+8-trunStart {
		t.Fatalf("data_offset = %d, want %d (mdat payload at %d)", dataOffset, moofEnd+8-trunStart, moofEnd+8)
	}
	return seq, sampleCount, dataOffset
}

// TestFmp4FragmentSequence verifies the mfhd sequence_number is a monotonic
// per-fragment counter (1, 2, 3, ...) and NOT the track time.
func TestFmp4FragmentSequence(t *testing.T) {
	var frags [][]byte
	mux := newFmp4Muxer(30, func(b []byte) {
		frags = append(frags, append([]byte{}, b...))
	}, func(b []byte) {
		// init segment; routed here (not into frags) so frags holds fragments only
	})
	mux.noteSPSPPS(testIDR())
	for i := 0; i < 3; i++ {
		mux.bumpFrame()
		mux.feed(testIDR()) // each IDR closes its fragment
	}
	if len(frags) != 3 {
		t.Fatalf("fragments = %d, want 3", len(frags))
	}
	for i, frag := range frags {
		seq, count, _ := parseMoof(t, frag)
		if seq != uint32(i+1) {
			t.Fatalf("fragment %d: mfhd sequence = %d, want %d (monotonic counter)", i, seq, i+1)
		}
		if count != 1 {
			t.Fatalf("fragment %d: sample count = %d, want 1", i, count)
		}
	}
}

// TestFmp4FragmentCap verifies a run of non-IDR AUs is cut at
// maxFragmentFrames (the sample cap), not only at IDR boundaries.
func TestFmp4FragmentCap(t *testing.T) {
	var frags [][]byte
	mux := newFmp4Muxer(30, func(b []byte) {
		frags = append(frags, append([]byte{}, b...))
	}, func(b []byte) {
		// init segment; routed here (not into frags) so frags holds fragments only
	})
	mux.noteSPSPPS(testIDR())
	for i := 0; i < maxFragmentFrames; i++ {
		mux.bumpFrame()
		mux.feed(testNonIDR())
	}
	if len(frags) != 1 {
		t.Fatalf("fragments = %d, want 1 (cut at the %d-sample cap)", len(frags), maxFragmentFrames)
	}
	_, count, _ := parseMoof(t, frags[0])
	if count != maxFragmentFrames {
		t.Fatalf("sample count = %d, want %d", count, maxFragmentFrames)
	}
}

// TestServeStreamLateJoinerInit verifies a /stream client that connects AFTER
// the init segment was built receives the init before its first live fragment
// (the Phase 0 late-joiner fix). Subscriber A (connected before the init was
// built) receives it via the broadcast; subscriber B (late joiner) receives it
// from the stored copy written by ServeStream.
func TestServeStreamLateJoinerInit(t *testing.T) {
	rs := NewRelayServer(config{DisableCapture: true})
	srv := httptest.NewServer(rs)
	defer srv.Close()
	init := []byte("ftypinitseg-moov")

	// ServeStream only sends the response headers on its first w.Write+Flush
	// (the init segment), so a synchronous http.Get would block until the init
	// is built. Run each GET in a goroutine and wait for the headers on a
	// channel.
	startGet := func() chan *http.Response {
		ch := make(chan *http.Response, 1)
		go func() {
			resp, err := http.Get(srv.URL + "/stream")
			if err != nil {
				ch <- nil
				return
			}
			ch <- resp
		}()
		return ch
	}
	waitResp := func(t *testing.T, ch chan *http.Response) *http.Response {
		t.Helper()
		select {
		case resp := <-ch:
			if resp == nil {
				t.Fatal("connect failed")
			}
			return resp
		case <-time.After(5 * time.Second):
			t.Fatal("connect timed out (headers never sent)")
		}
		return nil
	}

	// Subscriber A: registered BEFORE the init is built, so it receives the
	// init via storeInitSegment's broadcast.
	chA := startGet()
	// Poll until subscriber A is registered (the ServeStream goroutine adds it
	// after the handshake; a read before that would block on no data).
	deadline := time.Now().Add(2 * time.Second)
	for {
		rs.stream.mu.Lock()
		n := len(rs.stream.clients)
		rs.stream.mu.Unlock()
		if n >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("subscriber A never registered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Build the init (the capture pump's emitInit path). storeInitSegment
	// stores it for late joiners and broadcasts it to subscriber A, which
	// unblocks A's http.Get (headers sent) and delivers the init.
	rs.storeInitSegment(init)
	respA := waitResp(t, chA)
	defer respA.Body.Close()
	if got := readN(t, respA.Body, len(init)); !bytes.Equal(got, init) {
		t.Fatalf("subscriber A (broadcast) = %q, want the init", got)
	}

	// Subscriber B (late joiner): connected AFTER the init was built. It must
	// receive the stored init before any live fragment.
	chB := startGet()
	respB := waitResp(t, chB)
	defer respB.Body.Close()
	if got := readN(t, respB.Body, len(init)); !bytes.Equal(got, init) {
		t.Fatalf("subscriber B (late joiner) = %q, want the init", got)
	}

	// Broadcast a live fragment; subscriber B must have received the init
	// THEN the fragment (init first).
	frag := []byte("moof-fragment")
	rs.stream.write(frag)
	if got := readN(t, respB.Body, len(frag)); !bytes.Equal(got, frag) {
		t.Fatalf("subscriber B fragment = %q, want %q", got, frag)
	}
}

// readN reads exactly n bytes from r, failing the test on error or EOF.
func readN(t *testing.T, r io.Reader, n int) []byte {
	t.Helper()
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("read %d bytes: %v", n, err)
	}
	return buf
}

// TestStoreInitSegmentCopy verifies storeInitSegment stores a copy (the pump's
// buffer may be reused) and initSegment returns a stable copy.
func TestStoreInitSegmentCopy(t *testing.T) {
	rs := NewRelayServer(config{DisableCapture: true})
	buf := make([]byte, 16)
	for i := range buf {
		buf[i] = byte(i)
	}
	rs.storeInitSegment(buf)
	// Mutate the original buffer; the stored copy must be unaffected.
	for i := range buf {
		buf[i] = 0xFF
	}
	got := rs.initSegment()
	if got == nil {
		t.Fatal("initSegment is nil after storeInitSegment")
	}
	for i, b := range got {
		if b != byte(i) {
			t.Fatalf("stored init[%d] = %d, want %d (copy was not independent)", i, b, i)
		}
	}
}
