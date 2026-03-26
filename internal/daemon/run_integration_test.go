package daemon

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	exporter "location_logger/internal/export"
	"location_logger/internal/storage"
)

func TestDaemonIntegrationBufferingCompactionAndExport(t *testing.T) {
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "fake-location.sh")
	counterPath := filepath.Join(dir, "counter.txt")

	script := fmt.Sprintf(`#!/bin/sh
COUNTER_FILE=%q
if [ ! -f "$COUNTER_FILE" ]; then
  echo 0 > "$COUNTER_FILE"
fi
n=$(cat "$COUNTER_FILE")
n=$((n + 1))
echo "$n" > "$COUNTER_FILE"
if [ "$n" -le 6 ]; then
  printf '%%s\n' '{"latitude":50.1,"longitude":6.1,"provider":"gps"}'
  exit 0
fi
echo fail >&2
exit 1
`, counterPath)

	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake location script: %v", err)
	}

	cfg := Config{
		Interval:      80 * time.Millisecond,
		BufferSize:    3,
		OutputPath:    filepath.Join(dir, "data.bin"),
		CompactAfter:  2,
		LocationCmd:   scriptPath,
		SampleTimeout: 2 * time.Second,
		PendingCap:    30,
		LockPath:      filepath.Join(dir, "daemon.lock"),
		PIDPath:       filepath.Join(dir, "daemon.pid"),
		LogPath:       filepath.Join(dir, "daemon.log"),
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- Run(ctx, cfg)
	}()

	if err := waitForFileExists(cfg.PIDPath, 4*time.Second); err != nil {
		cancel()
		t.Fatalf("pid file not created: %v", err)
	}

	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		recordCount := 0
		if records, err := storage.ReadAll(cfg.OutputPath); err == nil {
			recordCount = len(records)
		}

		logData, _ := os.ReadFile(cfg.LogPath)
		hasFailure := strings.Contains(string(logData), "sample failed:")
		if recordCount >= 6 && hasFailure {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for daemon run to finish")
	}

	if _, err := os.Stat(cfg.PIDPath); !os.IsNotExist(err) {
		t.Fatalf("pid file should be removed after stop, stat error: %v", err)
	}

	records, err := storage.ReadAll(cfg.OutputPath)
	if err != nil {
		t.Fatalf("ReadAll after daemon run returned error: %v", err)
	}
	if len(records) != 6 {
		t.Fatalf("record count = %d, expected 6", len(records))
	}

	blockCount, err := countBlocks(cfg.OutputPath)
	if err != nil {
		t.Fatalf("countBlocks returned error: %v", err)
	}
	if blockCount != 1 {
		t.Fatalf("block count = %d, expected 1 after compaction", blockCount)
	}

	logData, err := os.ReadFile(cfg.LogPath)
	if err != nil {
		t.Fatalf("read daemon log returned error: %v", err)
	}
	if !strings.Contains(string(logData), "sample failed:") {
		t.Fatal("daemon log does not contain sample failure entries")
	}
	failureLine := regexp.MustCompile(`(?m)^\d{4}-\d{2}-\d{2}T.* sample failed:`)
	if !failureLine.Match(logData) {
		t.Fatalf("daemon log does not contain expected timestamped failure line:\n%s", string(logData))
	}

	csvPath := filepath.Join(dir, "out.csv")
	if err := exporter.Export(cfg.OutputPath, csvPath); err != nil {
		t.Fatalf("Export to csv returned error: %v", err)
	}

	csvData, err := os.ReadFile(csvPath)
	if err != nil {
		t.Fatalf("read exported csv returned error: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(csvData)), "\n")
	if len(lines) != 7 {
		t.Fatalf("csv line count = %d, expected 7", len(lines))
	}
	if lines[0] != "timestamp_utc,latitude,longitude,altitude,accuracy,vertical_accuracy,bearing,speed,elapsedMs,provider" {
		t.Fatalf("unexpected csv header: %s", lines[0])
	}
}

func countBlocks(path string) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	magic := make([]byte, len(storage.FileMagic))
	if _, err := io.ReadFull(file, magic); err != nil {
		return 0, err
	}
	if string(magic) != storage.FileMagic {
		return 0, fmt.Errorf("invalid magic %q", string(magic))
	}

	count := 0
	header := make([]byte, 12)
	for {
		if _, err := io.ReadFull(file, header); err != nil {
			if err == io.EOF {
				break
			}
			if err == io.ErrUnexpectedEOF {
				return 0, err
			}
			return 0, err
		}

		compressedLen := binary.LittleEndian.Uint32(header[0:4])
		if compressedLen == 0 {
			return 0, fmt.Errorf("invalid block length 0")
		}

		if _, err := file.Seek(int64(compressedLen), io.SeekCurrent); err != nil {
			return 0, err
		}
		count++
	}

	return count, nil
}
