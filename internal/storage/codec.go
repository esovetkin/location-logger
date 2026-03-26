package storage

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"time"
)

const (
	FileMagic = "LLOG1"

	maskLatitude uint16 = 1 << iota
	maskLongitude
	maskAltitude
	maskAccuracy
	maskVerticalAccuracy
	maskBearing
	maskSpeed
	maskElapsedMs
	maskProvider
)

const (
	maxBlockRawLen        = 64 << 20
	maxBlockCompressedLen = 64 << 20
)

var (
	ErrInvalidMagic = errors.New("invalid file magic")
)

func encodeBatch(records []Record) ([]byte, error) {
	var out bytes.Buffer
	if err := writeUvarint(&out, uint64(len(records))); err != nil {
		return nil, err
	}

	for i := range records {
		if err := encodeRecord(&out, records[i]); err != nil {
			return nil, fmt.Errorf("encode record %d: %w", i, err)
		}
	}

	return out.Bytes(), nil
}

func decodeBatch(raw []byte) ([]Record, error) {
	reader := bytes.NewReader(raw)
	count, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("read record count: %w", err)
	}

	records := make([]Record, 0, count)
	for i := uint64(0); i < count; i++ {
		rec, err := decodeRecord(reader)
		if err != nil {
			return nil, fmt.Errorf("decode record %d: %w", i, err)
		}
		records = append(records, rec)
	}

	if reader.Len() != 0 {
		return nil, fmt.Errorf("decoded batch has %d trailing bytes", reader.Len())
	}

	return records, nil
}

func encodeRecord(w io.Writer, rec Record) error {
	timestampMillis := rec.TimestampUTC.UTC().UnixMilli()
	if err := binary.Write(w, binary.LittleEndian, timestampMillis); err != nil {
		return fmt.Errorf("write timestamp: %w", err)
	}

	var mask uint16
	if !math.IsNaN(rec.Latitude) {
		mask |= maskLatitude
	}
	if !math.IsNaN(rec.Longitude) {
		mask |= maskLongitude
	}
	if !math.IsNaN(float64(rec.Altitude)) {
		mask |= maskAltitude
	}
	if !math.IsNaN(float64(rec.Accuracy)) {
		mask |= maskAccuracy
	}
	if !math.IsNaN(float64(rec.VerticalAccuracy)) {
		mask |= maskVerticalAccuracy
	}
	if !math.IsNaN(float64(rec.Bearing)) {
		mask |= maskBearing
	}
	if !math.IsNaN(float64(rec.Speed)) {
		mask |= maskSpeed
	}
	if rec.ElapsedMs != nil {
		mask |= maskElapsedMs
	}
	if rec.Provider != nil {
		mask |= maskProvider
	}

	if err := binary.Write(w, binary.LittleEndian, mask); err != nil {
		return fmt.Errorf("write present mask: %w", err)
	}

	if mask&maskLatitude != 0 {
		if err := binary.Write(w, binary.LittleEndian, rec.Latitude); err != nil {
			return fmt.Errorf("write latitude: %w", err)
		}
	}
	if mask&maskLongitude != 0 {
		if err := binary.Write(w, binary.LittleEndian, rec.Longitude); err != nil {
			return fmt.Errorf("write longitude: %w", err)
		}
	}
	if mask&maskAltitude != 0 {
		if err := binary.Write(w, binary.LittleEndian, rec.Altitude); err != nil {
			return fmt.Errorf("write altitude: %w", err)
		}
	}
	if mask&maskAccuracy != 0 {
		if err := binary.Write(w, binary.LittleEndian, rec.Accuracy); err != nil {
			return fmt.Errorf("write accuracy: %w", err)
		}
	}
	if mask&maskVerticalAccuracy != 0 {
		if err := binary.Write(w, binary.LittleEndian, rec.VerticalAccuracy); err != nil {
			return fmt.Errorf("write vertical accuracy: %w", err)
		}
	}
	if mask&maskBearing != 0 {
		if err := binary.Write(w, binary.LittleEndian, rec.Bearing); err != nil {
			return fmt.Errorf("write bearing: %w", err)
		}
	}
	if mask&maskSpeed != 0 {
		if err := binary.Write(w, binary.LittleEndian, rec.Speed); err != nil {
			return fmt.Errorf("write speed: %w", err)
		}
	}
	if mask&maskElapsedMs != 0 {
		if err := binary.Write(w, binary.LittleEndian, *rec.ElapsedMs); err != nil {
			return fmt.Errorf("write elapsedMs: %w", err)
		}
	}
	if mask&maskProvider != 0 {
		provider := *rec.Provider
		if err := writeUvarint(w, uint64(len(provider))); err != nil {
			return fmt.Errorf("write provider length: %w", err)
		}
		if _, err := io.WriteString(w, provider); err != nil {
			return fmt.Errorf("write provider: %w", err)
		}
	}

	return nil
}

func decodeRecord(r *bytes.Reader) (Record, error) {
	var timestampMillis int64
	if err := binary.Read(r, binary.LittleEndian, &timestampMillis); err != nil {
		return Record{}, fmt.Errorf("read timestamp: %w", err)
	}

	var mask uint16
	if err := binary.Read(r, binary.LittleEndian, &mask); err != nil {
		return Record{}, fmt.Errorf("read present mask: %w", err)
	}

	rec := NewRecordWithMissing(time.UnixMilli(timestampMillis).UTC())

	if mask&maskLatitude != 0 {
		if err := binary.Read(r, binary.LittleEndian, &rec.Latitude); err != nil {
			return Record{}, fmt.Errorf("read latitude: %w", err)
		}
	}
	if mask&maskLongitude != 0 {
		if err := binary.Read(r, binary.LittleEndian, &rec.Longitude); err != nil {
			return Record{}, fmt.Errorf("read longitude: %w", err)
		}
	}
	if mask&maskAltitude != 0 {
		if err := binary.Read(r, binary.LittleEndian, &rec.Altitude); err != nil {
			return Record{}, fmt.Errorf("read altitude: %w", err)
		}
	}
	if mask&maskAccuracy != 0 {
		if err := binary.Read(r, binary.LittleEndian, &rec.Accuracy); err != nil {
			return Record{}, fmt.Errorf("read accuracy: %w", err)
		}
	}
	if mask&maskVerticalAccuracy != 0 {
		if err := binary.Read(r, binary.LittleEndian, &rec.VerticalAccuracy); err != nil {
			return Record{}, fmt.Errorf("read vertical accuracy: %w", err)
		}
	}
	if mask&maskBearing != 0 {
		if err := binary.Read(r, binary.LittleEndian, &rec.Bearing); err != nil {
			return Record{}, fmt.Errorf("read bearing: %w", err)
		}
	}
	if mask&maskSpeed != 0 {
		if err := binary.Read(r, binary.LittleEndian, &rec.Speed); err != nil {
			return Record{}, fmt.Errorf("read speed: %w", err)
		}
	}
	if mask&maskElapsedMs != 0 {
		var elapsed uint32
		if err := binary.Read(r, binary.LittleEndian, &elapsed); err != nil {
			return Record{}, fmt.Errorf("read elapsedMs: %w", err)
		}
		rec.ElapsedMs = &elapsed
	}
	if mask&maskProvider != 0 {
		length, err := binary.ReadUvarint(r)
		if err != nil {
			return Record{}, fmt.Errorf("read provider length: %w", err)
		}
		if length > uint64(r.Len()) {
			return Record{}, fmt.Errorf("provider length %d exceeds %d remaining bytes", length, r.Len())
		}
		providerBytes := make([]byte, length)
		if _, err := io.ReadFull(r, providerBytes); err != nil {
			return Record{}, fmt.Errorf("read provider: %w", err)
		}
		provider := string(providerBytes)
		rec.Provider = &provider
	}

	return rec, nil
}

func encodeCompressedBlock(records []Record) ([]byte, uint32, uint32, error) {
	raw, err := encodeBatch(records)
	if err != nil {
		return nil, 0, 0, err
	}

	compressed, err := compressRaw(raw)
	if err != nil {
		return nil, 0, 0, err
	}

	if len(raw) > maxBlockRawLen {
		return nil, 0, 0, fmt.Errorf("raw block size %d exceeds maximum %d", len(raw), maxBlockRawLen)
	}
	if len(compressed) > maxBlockCompressedLen {
		return nil, 0, 0, fmt.Errorf("compressed block size %d exceeds maximum %d", len(compressed), maxBlockCompressedLen)
	}

	crc := crc32.ChecksumIEEE(raw)
	return compressed, uint32(len(raw)), crc, nil
}

func decodeCompressedBlock(compressed []byte, rawLen uint32, expectedCRC uint32) ([]Record, error) {
	raw, err := decompressRaw(compressed)
	if err != nil {
		return nil, err
	}

	if uint32(len(raw)) != rawLen {
		return nil, fmt.Errorf("raw length mismatch: got %d, expected %d", len(raw), rawLen)
	}

	crc := crc32.ChecksumIEEE(raw)
	if crc != expectedCRC {
		return nil, fmt.Errorf("crc mismatch: got %08x, expected %08x", crc, expectedCRC)
	}

	records, err := decodeBatch(raw)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func compressRaw(raw []byte) ([]byte, error) {
	var out bytes.Buffer
	writer, err := gzip.NewWriterLevel(&out, gzip.BestCompression)
	if err != nil {
		return nil, fmt.Errorf("create gzip writer: %w", err)
	}

	if _, err := writer.Write(raw); err != nil {
		_ = writer.Close()
		return nil, fmt.Errorf("gzip write: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("gzip close: %w", err)
	}

	return out.Bytes(), nil
}

func decompressRaw(compressed []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("open gzip stream: %w", err)
	}
	defer reader.Close()

	raw, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read gzip stream: %w", err)
	}
	return raw, nil
}

func writeUvarint(w io.Writer, value uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], value)
	if _, err := w.Write(buf[:n]); err != nil {
		return fmt.Errorf("write uvarint: %w", err)
	}
	return nil
}
