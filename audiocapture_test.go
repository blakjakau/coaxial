package main

import (
	"bytes"
	"testing"
)

// TestOggDemuxerSinglePacketPerPage feeds the demuxer a stream of Ogg pages,
// each holding a single opus packet, and asserts every packet is emitted
// intact (in order) and the OpusHead/OpusTags headers are skipped.
func TestOggDemuxerSinglePacketPerPage(t *testing.T) {
	head := []byte("OpusHead\x01\x08\x00\x00\x00\x00\x00\x00")
	tags := []byte("OpusTags\x00\x00\x00\x00")
	pkt1 := bytes.Repeat([]byte{0xAB}, 100)
	pkt2 := bytes.Repeat([]byte{0xCD}, 200)
	pkt3 := bytes.Repeat([]byte{0xEF}, 300)

	// Assemble the Ogg stream: header pages + audio packet pages.
	var stream []byte
	stream = append(stream, buildOggPage([][]byte{head, tags}, 0, 0)...)
	stream = append(stream, buildOggPage([][]byte{pkt1}, 960, 1)...)
	stream = append(stream, buildOggPage([][]byte{pkt2}, 1920, 2)...)
	stream = append(stream, buildOggPage([][]byte{pkt3}, 2880, 3)...)

	var got [][]byte
	var headGot [][]byte
	d := newOggDemuxer(
		func(pkt []byte) {
			cp := make([]byte, len(pkt))
			copy(cp, pkt)
			got = append(got, cp)
		},
		func(head []byte) {
			cp := make([]byte, len(head))
			copy(cp, head)
			headGot = append(headGot, cp)
		},
	)
	d.feed(stream)

	if len(got) != 3 {
		t.Fatalf("expected 3 audio packets, got %d", len(got))
	}
	// The OpusHead is routed to onHead (not onPkt) and emitted exactly once.
	if len(headGot) != 1 || !bytes.Equal(headGot[0], head) {
		t.Errorf("OpusHead not emitted via onHead: got %d, want 1 (matching the head)", len(headGot))
	}
	if !bytes.Equal(got[0], pkt1) {
		t.Errorf("pkt1 mismatch: got %d bytes, want %d", len(got[0]), len(pkt1))
	}
	if !bytes.Equal(got[1], pkt2) {
		t.Errorf("pkt2 mismatch: got %d bytes, want %d", len(got[1]), len(pkt2))
	}
	if !bytes.Equal(got[2], pkt3) {
		t.Errorf("pkt3 mismatch: got %d bytes, want %d", len(got[2]), len(pkt3))
	}
}

// TestOggDemuxerMultiPacketPage feeds a page holding several packets and
// asserts they are all emitted in order.
func TestOggDemuxerMultiPacketPage(t *testing.T) {
	pkt1 := bytes.Repeat([]byte{0x11}, 50)
	pkt2 := bytes.Repeat([]byte{0x22}, 50)
	pkt3 := bytes.Repeat([]byte{0x33}, 50)

	stream := buildOggPage([][]byte{pkt1, pkt2, pkt3}, 960, 0)

	var got [][]byte
	d := newOggDemuxer(
		func(pkt []byte) {
			cp := make([]byte, len(pkt))
			copy(cp, pkt)
			got = append(got, cp)
		},
		nil, // no head in this stream
	)
	d.feed(stream)

	if len(got) != 3 {
		t.Fatalf("expected 3 packets, got %d", len(got))
	}
	if !bytes.Equal(got[0], pkt1) || !bytes.Equal(got[1], pkt2) || !bytes.Equal(got[2], pkt3) {
		t.Errorf("packet contents mismatch")
	}
}

// TestOggDemuxerChunkedFeed feeds the same stream in small chunks (simulating
// the stdout pump reading in 64KB blocks that don't align to page
// boundaries) and asserts the demuxer reassembles the packets correctly.
func TestOggDemuxerChunkedFeed(t *testing.T) {
	head := []byte("OpusHead\x01\x08\x00\x00\x00\x00\x00\x00")
	tags := []byte("OpusTags\x00\x00\x00\x00")
	var pkts [][]byte
	for i := 0; i < 20; i++ {
		// Vary the packet size (some small, some large) to exercise the
		// lacing table across many segments.
		size := 30 + (i * 37) % 200
		pkts = append(pkts, bytes.Repeat([]byte{byte(i)}, size))
	}

	var stream []byte
	stream = append(stream, buildOggPage([][]byte{head, tags}, 0, 0)...)
	for i, pkt := range pkts {
		stream = append(stream, buildOggPage([][]byte{pkt}, int64(960*(i+1)), uint32(i+1))...)
	}

	var got [][]byte
	var headGot [][]byte
	d := newOggDemuxer(
		func(pkt []byte) {
			cp := make([]byte, len(pkt))
			copy(cp, pkt)
			got = append(got, cp)
		},
		func(head []byte) {
			cp := make([]byte, len(head))
			copy(cp, head)
			headGot = append(headGot, cp)
		},
	)
	// Feed in 7-byte chunks (deliberately misaligned with page boundaries).
	for off := 0; off < len(stream); off += 7 {
		end := off + 7
		if end > len(stream) {
			end = len(stream)
		}
		d.feed(stream[off:end])
	}

	if len(got) != len(pkts) {
		t.Fatalf("expected %d packets, got %d", len(pkts), len(got))
	}
	for i, want := range pkts {
		if !bytes.Equal(got[i], want) {
			t.Errorf("packet %d mismatch: got %d bytes, want %d", i, len(got[i]), len(want))
		}
	}
	// The head survived the chunked (misaligned) feed and reached onHead once.
	if len(headGot) != 1 || !bytes.Equal(headGot[0], head) {
		t.Errorf("OpusHead not emitted via onHead after chunked feed: got %d", len(headGot))
	}
}

// TestOggDemuxerLargePacket feeds a packet larger than 255 bytes (multiple
// segments, lacing 255) and asserts it is reassembled correctly.
func TestOggDemuxerLargePacket(t *testing.T) {
	// A 1000-byte packet: 3 full 255-byte segments (lacing 255) + a final
	// 235-byte segment (lacing 235, = segment size per oggmux convention).
	pkt := bytes.Repeat([]byte{0x77}, 1000)
	stream := buildOggPage([][]byte{pkt}, 960, 0)

	var got [][]byte
	d := newOggDemuxer(
		func(pkt []byte) {
			cp := make([]byte, len(pkt))
			copy(cp, pkt)
			got = append(got, cp)
		},
		nil, // no head in this stream
	)
	d.feed(stream)

	if len(got) != 1 {
		t.Fatalf("expected 1 packet, got %d", len(got))
	}
	if !bytes.Equal(got[0], pkt) {
		t.Errorf("large packet mismatch: got %d bytes, want %d", len(got[0]), len(pkt))
	}
}

// TestOggPageEnd verifies the page-length computation against a known page.
func TestOggPageEnd(t *testing.T) {
	pkt := bytes.Repeat([]byte{0x55}, 100)
	page := buildOggPage([][]byte{pkt}, 960, 0)
	if got := oggPageEnd(page); got != len(page) {
		t.Errorf("oggPageEnd = %d, want %d", got, len(page))
	}
}
