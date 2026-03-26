package storage

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestEncodeDecodeBatchRoundTripMaskAndNaN(t *testing.T) {
	provider := "gps"
	elapsed := uint32(6)

	records := []Record{
		{
			TimestampUTC:     time.Unix(1711449600, 123000000).UTC(),
			Latitude:         50.86697859,
			Longitude:        6.12225608,
			Altitude:         184.26807,
			Accuracy:         10.052665,
			VerticalAccuracy: 4.0,
			Bearing:          0,
			Speed:            0,
			ElapsedMs:        &elapsed,
			Provider:         &provider,
		},
		func() Record {
			rec := NewRecordWithMissing(time.Unix(1711449700, 0).UTC())
			rec.Latitude = 51.0
			rec.Longitude = 7.0
			return rec
		}(),
	}

	raw, err := encodeBatch(records)
	if err != nil {
		t.Fatalf("encodeBatch returned error: %v", err)
	}

	decoded, err := decodeBatch(raw)
	if err != nil {
		t.Fatalf("decodeBatch returned error: %v", err)
	}

	if len(decoded) != len(records) {
		t.Fatalf("decoded length = %d, expected %d", len(decoded), len(records))
	}

	if decoded[0].TimestampUTC.UnixMilli() != records[0].TimestampUTC.UnixMilli() {
		t.Fatalf("timestamp mismatch: got %d expected %d", decoded[0].TimestampUTC.UnixMilli(), records[0].TimestampUTC.UnixMilli())
	}
	if decoded[0].Latitude != records[0].Latitude || decoded[0].Longitude != records[0].Longitude {
		t.Fatalf("coordinate mismatch: got (%v,%v) expected (%v,%v)", decoded[0].Latitude, decoded[0].Longitude, records[0].Latitude, records[0].Longitude)
	}
	if decoded[0].ElapsedMs == nil || *decoded[0].ElapsedMs != elapsed {
		t.Fatalf("elapsedMs mismatch: got %v expected %d", decoded[0].ElapsedMs, elapsed)
	}
	if decoded[0].Provider == nil || *decoded[0].Provider != provider {
		t.Fatalf("provider mismatch: got %v expected %q", decoded[0].Provider, provider)
	}

	if math.IsNaN(decoded[1].Latitude) || math.IsNaN(decoded[1].Longitude) {
		t.Fatalf("coordinates should be present in second record")
	}
	if !math.IsNaN(float64(decoded[1].Altitude)) {
		t.Fatalf("altitude should be NaN when missing")
	}
	if decoded[1].ElapsedMs != nil {
		t.Fatalf("elapsedMs should be nil when missing")
	}
	if decoded[1].Provider != nil {
		t.Fatalf("provider should be nil when missing")
	}
}

func TestDecodeCompressedBlockCRCMismatch(t *testing.T) {
	records := []Record{NewRecordWithMissing(time.Unix(1711449600, 0).UTC())}
	compressed, rawLen, crc, err := encodeCompressedBlock(records)
	if err != nil {
		t.Fatalf("encodeCompressedBlock returned error: %v", err)
	}

	_, err = decodeCompressedBlock(compressed, rawLen, crc+1)
	if err == nil {
		t.Fatal("expected CRC mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "crc mismatch") {
		t.Fatalf("expected crc mismatch error, got: %v", err)
	}
}
