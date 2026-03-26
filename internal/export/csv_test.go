package export

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"location_logger/internal/storage"
)

func TestExportWritesNaNAndEmptyCells(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "data.bin")
	output := filepath.Join(dir, "out.csv")

	rec := storage.NewRecordWithMissing(time.Unix(1711449600, 0).UTC())
	rec.Latitude = 50.0
	rec.Longitude = 6.0

	if err := storage.WriteAllCompressed(input, []storage.Record{rec}, 1); err != nil {
		t.Fatalf("WriteAllCompressed returned error: %v", err)
	}

	if err := Export(input, output); err != nil {
		t.Fatalf("Export returned error: %v", err)
	}

	csvData, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("read output csv returned error: %v", err)
	}

	text := string(csvData)
	if !strings.Contains(text, "NaN") {
		t.Fatalf("expected NaN values in csv output, got:\n%s", text)
	}

	lines := strings.Split(strings.TrimSpace(text), "\n")
	if len(lines) != 2 {
		t.Fatalf("csv line count = %d, expected 2", len(lines))
	}
	if !strings.HasSuffix(lines[1], ",,") {
		t.Fatalf("expected missing elapsedMs/provider empty cells, row: %s", lines[1])
	}
}

func TestExportStdoutFallbackWhenOutputMissing(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "data.bin")

	provider := "gps"
	rec := storage.NewRecordWithMissing(time.Unix(1711449600, 0).UTC())
	rec.Latitude = 1.0
	rec.Longitude = 2.0
	rec.Provider = &provider

	if err := storage.WriteAllCompressed(input, []storage.Record{rec}, 1); err != nil {
		t.Fatalf("WriteAllCompressed returned error: %v", err)
	}

	var out bytes.Buffer
	if err := exportWithWriter(input, "", &out); err != nil {
		t.Fatalf("exportWithWriter returned error: %v", err)
	}

	text := out.String()
	if !strings.Contains(text, "timestamp_utc,latitude,longitude,altitude,accuracy,vertical_accuracy,bearing,speed,elapsedMs,provider") {
		t.Fatalf("missing csv header in stdout output:\n%s", text)
	}
	if !strings.Contains(text, ",gps") {
		t.Fatalf("missing provider value in stdout output:\n%s", text)
	}
}
