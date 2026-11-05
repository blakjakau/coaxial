package main

import (
	"bytes"
	"testing"
)

// sc4 is the 4-byte Annex-B start code h264parse emits.
var sc4 = []byte{0, 0, 0, 1}
// sc3 is the 3-byte Annex-B start code (accepted for robustness).
var sc3 = []byte{0, 0, 1}

// nal builds a NAL unit with the given type (low 5 bits) and payload.
func nal(t byte, payload ...byte) []byte {
	out := append([]byte{t & 0x1F}, payload...)
	return out
}

// join concatenates byte slices.
func join(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// TestFeedKeyframeThenPFrame feeds one chunk containing a keyframe access
// unit (SPS+PPS+IDR slice) followed by a pframe access unit (non-IDR slice).
// This is the exact case that previously spun forever (residual beginning
// with a 4-byte start code). The keyframe AU must carry its SPS+PPS config
// NALs so a mid-stream client can build its decoder config.
func TestFeedKeyframeThenPFrame(t *testing.T) {
	au1 := join(sc4, nal(7, 0x42, 0xC0), sc4, nal(8, 0xCE), sc4, nal(5, 0xAA, 0xBB))
	au2 := join(sc4, nal(1, 0xCC, 0xDD))
	chunk := join(au1, au2)

	acc := newAccessUnitAccumulator()
	var got [][]byte
	// au1 is emitted on feed; au2 is the trailing access unit (no start code
	// after its slice), so it is held back until flush.
	acc.feed(chunk, func(au []byte) {
		got = append(got, append([]byte{}, au...))
	})
	acc.flush(func(au []byte) {
		got = append(got, append([]byte{}, au...))
	})
	if len(got) != 2 {
		t.Fatalf("want 2 access units, got %d", len(got))
	}
	if !bytes.Equal(got[0], au1) {
		t.Errorf("AU1 mismatch:\n got %x\nwant %x", got[0], au1)
	}
	if !bytes.Equal(got[1], au2) {
		t.Errorf("AU2 mismatch:\n got %x\nwant %x", got[1], au2)
	}
}

// TestFeedSplitAcrossChunks feeds a keyframe access unit in two halves (the
// split landing mid-NAL) and verifies the complete unit is emitted only once
// the slice's start code arrives with the second half.
func TestFeedSplitAcrossChunks(t *testing.T) {
	au := join(sc4, nal(7, 0x42, 0xC0), sc4, nal(8, 0xCE), sc4, nal(5, 0xAA))
	// Split inside the PPS NAL: the first half has no slice start code, so
	// nothing is emitted yet. (SPS NAL is 3 bytes: type + 2 payload.)
	half := 4 + 3 + 1 // sc4 + SPS(3) + 1 byte of PPS NAL

	acc := newAccessUnitAccumulator()
	var got [][]byte
	acc.feed(au[:half], func(au []byte) {
		got = append(got, append([]byte{}, au...))
	})
	if len(got) != 0 {
		t.Fatalf("no access unit should be emitted from an incomplete chunk, got %d", len(got))
	}
	// Feed the remainder; the IDR slice's start code completes the AU. The
	// AU is the trailing access unit (no start code after its slice), so it
	// is held back until flush.
	acc.feed(au[half:], func(au []byte) {
		got = append(got, append([]byte{}, au...))
	})
	acc.flush(func(au []byte) {
		got = append(got, append([]byte{}, au...))
	})
	if len(got) != 1 {
		t.Fatalf("want 1 access unit after the split completes, got %d", len(got))
	}
	if !bytes.Equal(got[0], au) {
		t.Errorf("AU mismatch:\n got %x\nwant %x", got[0], au)
	}
}

// TestFeedThreeByteStartCodes verifies 3-byte start codes are accepted and
// emitted with their leading start code. An access unit contains exactly one
// slice NAL, so the three slice NALs below are three separate AUs.
func TestFeedThreeByteStartCodes(t *testing.T) {
	au1 := join(sc3, nal(5, 0x11))
	au2 := join(sc3, nal(1, 0x22))
	au3 := join(sc3, nal(1, 0x33))

	acc := newAccessUnitAccumulator()
	var got [][]byte
	// au1 and au2 are emitted on feed; au3 is the trailing access unit, held
	// back until flush.
	acc.feed(join(au1, au2, au3), func(au []byte) {
		got = append(got, append([]byte{}, au...))
	})
	acc.flush(func(au []byte) {
		got = append(got, append([]byte{}, au...))
	})
	if len(got) != 3 {
		t.Fatalf("want 3 access units, got %d", len(got))
	}
	if !bytes.Equal(got[0], au1) {
		t.Errorf("AU1 mismatch:\n got %x\nwant %x", got[0], au1)
	}
	if !bytes.Equal(got[1], au2) {
		t.Errorf("AU2 mismatch:\n got %x\nwant %x", got[1], au2)
	}
	if !bytes.Equal(got[2], au3) {
		t.Errorf("AU3 mismatch:\n got %x\nwant %x", got[2], au3)
	}
}

// TestFeedLargeChunkNoSpin feeds a large, realistic-looking chunk (many
// access units, ~1MB) with a timeout guard against the historical infinite
// loop, and verifies the unit count.
func TestFeedLargeChunkNoSpin(t *testing.T) {
	const units = 2000
	var chunk []byte
	for i := 0; i < units; i++ {
		if i%30 == 0 {
			// Keyframe: SPS+PPS+IDR.
			chunk = join(chunk, sc4, nal(7, 0x42), sc4, nal(8, 0xCE), sc4, nal(5, 0xAA, 0xBB, 0xCC))
		} else {
			chunk = join(chunk, sc4, nal(1, 0xAA, 0xBB, 0xCC, 0xDD))
		}
	}

	acc := newAccessUnitAccumulator()
	count := 0
	// Direct call: if feed spins (the historical bug), go test's per-test
	// timeout catches the hang. The trailing access unit is held back until
	// flush (EOF), so feed emits units-1 and flush emits the last one.
	acc.feed(chunk, func(au []byte) { count++ })
	if count != units-1 {
		t.Fatalf("want %d access units from feed (last held back), got %d", units-1, count)
	}
	acc.flush(func(au []byte) { count++ })
	if count != units {
		t.Fatalf("want %d access units after flush, got %d", units, count)
	}
}
