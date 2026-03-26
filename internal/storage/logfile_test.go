package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReadAllCorruptedTrailingBlock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.bin")

	records := []Record{NewRecordWithMissing(time.Unix(1711449600, 0).UTC())}
	if err := WriteAllCompressed(path, records, 1); err != nil {
		t.Fatalf("WriteAllCompressed returned error: %v", err)
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open file for corruption returned error: %v", err)
	}
	defer file.Close()

	if _, err := file.Write([]byte{0x10, 0x00, 0x00}); err != nil {
		t.Fatalf("append corruption bytes returned error: %v", err)
	}

	_, err = ReadAll(path)
	if err == nil {
		t.Fatal("expected corruption error, got nil")
	}
	if !strings.Contains(err.Error(), "truncated block header") {
		t.Fatalf("expected truncated header error, got: %v", err)
	}
}

func TestEnsureLogFileAndAppendRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.bin")

	if err := EnsureLogFile(path); err != nil {
		t.Fatalf("EnsureLogFile returned error: %v", err)
	}

	rec := NewRecordWithMissing(time.Unix(1711449600, 0).UTC())
	rec.Latitude = 1.23
	rec.Longitude = 4.56

	if err := AppendBatch(path, []Record{rec}); err != nil {
		t.Fatalf("AppendBatch returned error: %v", err)
	}

	decoded, err := ReadAll(path)
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("decoded length = %d, expected 1", len(decoded))
	}
	if decoded[0].Latitude != rec.Latitude || decoded[0].Longitude != rec.Longitude {
		t.Fatalf("coordinates mismatch: got (%v,%v) expected (%v,%v)", decoded[0].Latitude, decoded[0].Longitude, rec.Latitude, rec.Longitude)
	}
}
