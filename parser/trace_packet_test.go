package parser

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildTracePacket assembles a t-stream packet: the common 8-byte header
// followed by the given fixed 16-byte XrdXrootdMonTrace entries.
func buildTracePacket(t *testing.T, entries ...[16]byte) []byte {
	t.Helper()

	plen := 8 + 16*len(entries)
	b := make([]byte, 0, plen)
	b = append(b, PacketTypeTrace, 0x01) // code 't', pseq
	b = binary.BigEndian.AppendUint16(b, uint16(plen))
	b = binary.BigEndian.AppendUint32(b, 1782994817) // stod (server start)
	for _, e := range entries {
		b = append(b, e[:]...)
	}
	return b
}

// TestTracePacketProducesNoFileRecords verifies that t-stream packets are
// accepted but never fed through the f-stream record walker. The t-stream
// consists of fixed 16-byte entries with no recType/recFlag/recSize header;
// misaligned parsing of those entries used to fabricate file-close records
// with garbage 64-bit byte counters (observed in production as PB/EiB-scale
// transfer spikes on records with filename "unknown").
func TestTracePacketProducesNoFileRecords(t *testing.T) {
	// Window mark entry (0xe0): the usual first entry of a trace buffer.
	window := [16]byte{0: 0xe0}
	binary.BigEndian.PutUint32(window[8:12], 1782994800)  // prev window end
	binary.BigEndian.PutUint32(window[12:16], 1782994830) // this window start

	// Close entry (0xc0): rRshift, wRshift, pad, rTot, wTot, dictid.
	closeEntry := [16]byte{0: 0xc0, 1: 0x02, 2: 0x01} // non-zero wRshift at byte 2:
	// bytes 2-3 previously misread as an f-stream recSize, starting the
	// misaligned walk that fabricated close records.
	binary.BigEndian.PutUint32(closeEntry[4:8], 0x2FCBE0C0)  // rTot
	binary.BigEndian.PutUint32(closeEntry[8:12], 0x2FC71A80) // wTot
	binary.BigEndian.PutUint32(closeEntry[12:16], 12235)     // dictid

	// Zero-heavy entry: a stray 0x00 first byte previously matched
	// RecTypeClose and became a fabricated 64-bit counter record.
	zeroHeavy := [16]byte{2: 0x00, 3: 0x20} // bytes 2-3 = 32, a "valid" recSize
	binary.BigEndian.PutUint32(zeroHeavy[8:12], 0x05002FD2)
	binary.BigEndian.PutUint32(zeroHeavy[12:16], 0x28C00000)

	cases := []struct {
		name    string
		entries [][16]byte
	}{
		{"window_only", [][16]byte{window}},
		{"window_and_close", [][16]byte{window, closeEntry}},
		{"misparse_bait", [][16]byte{closeEntry, zeroHeavy, closeEntry, zeroHeavy}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := buildTracePacket(t, tc.entries...)

			packet, err := ParsePacket(raw)
			require.NoError(t, err, "t-stream packets must still be accepted (shoveling mode)")
			require.NotNil(t, packet)

			assert.Equal(t, PacketTypeTrace, packet.PacketType)
			assert.Empty(t, packet.FileRecords,
				"t-stream packets must not produce file records; the f-stream walker fabricates garbage counters from 16-byte trace entries")
			assert.Equal(t, raw, packet.RawData, "raw data must be preserved for shoveling")
		})
	}
}
