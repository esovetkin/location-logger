package daemon

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"location_logger/internal/storage"
)

func TestSIGHUPFlushesPendingBuffer(t *testing.T) {
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
printf '%%s\n' '{"latitude":1.0,"longitude":2.0}'
`, counterPath)

	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake location script: %v", err)
	}

	cfg := Config{
		Interval:      80 * time.Millisecond,
		BufferSize:    50,
		OutputPath:    filepath.Join(dir, "data.bin"),
		CompactAfter:  100,
		LocationCmd:   scriptPath,
		SampleTimeout: 2 * time.Second,
		PendingCap:    500,
		LockPath:      filepath.Join(dir, "daemon.lock"),
		PIDPath:       filepath.Join(dir, "daemon.pid"),
		LogPath:       filepath.Join(dir, "daemon.log"),
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, cfg)
	}()

	if err := waitForFileExists(cfg.PIDPath, 3*time.Second); err != nil {
		cancel()
		t.Fatalf("pid file was not created: %v", err)
	}

	if err := waitForCounterAtLeast(counterPath, 2, 4*time.Second); err != nil {
		cancel()
		t.Fatalf("location command did not run enough times before SIGHUP: %v", err)
	}

	before, err := storage.ReadAll(cfg.OutputPath)
	if err != nil {
		cancel()
		t.Fatalf("ReadAll before SIGHUP returned error: %v", err)
	}
	if len(before) != 0 {
		cancel()
		t.Fatalf("expected no flushed records before SIGHUP, got %d", len(before))
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
		cancel()
		t.Fatalf("send SIGHUP failed: %v", err)
	}

	if err := waitForRecordCountAtLeast(cfg.OutputPath, 1, 4*time.Second); err != nil {
		cancel()
		t.Fatalf("pending buffer was not flushed on SIGHUP: %v", err)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Run to stop")
	}
}

func TestSIGUSR1ForcesRecompression(t *testing.T) {
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "data.bin")

	records := []storage.Record{
		func() storage.Record {
			rec := storage.NewRecordWithMissing(time.Unix(1711449600, 0).UTC())
			rec.Latitude = 1
			rec.Longitude = 2
			return rec
		}(),
		func() storage.Record {
			rec := storage.NewRecordWithMissing(time.Unix(1711449660, 0).UTC())
			rec.Latitude = 3
			rec.Longitude = 4
			return rec
		}(),
		func() storage.Record {
			rec := storage.NewRecordWithMissing(time.Unix(1711449720, 0).UTC())
			rec.Latitude = 5
			rec.Longitude = 6
			return rec
		}(),
	}
	if err := storage.WriteAllCompressed(outputPath, records, 1); err != nil {
		t.Fatalf("WriteAllCompressed returned error: %v", err)
	}

	cfg := Config{
		Interval:      100 * time.Millisecond,
		BufferSize:    50,
		OutputPath:    outputPath,
		CompactAfter:  100,
		LocationCmd:   "false",
		SampleTimeout: 2 * time.Second,
		PendingCap:    500,
		LockPath:      filepath.Join(dir, "daemon.lock"),
		PIDPath:       filepath.Join(dir, "daemon.pid"),
		LogPath:       filepath.Join(dir, "daemon.log"),
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, cfg)
	}()

	if err := waitForFileExists(cfg.PIDPath, 3*time.Second); err != nil {
		cancel()
		t.Fatalf("pid file was not created: %v", err)
	}

	if err := waitForBlockCountEquals(outputPath, 3, 3*time.Second); err != nil {
		cancel()
		t.Fatalf("initial block count check failed: %v", err)
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR1); err != nil {
		cancel()
		t.Fatalf("send SIGUSR1 failed: %v", err)
	}

	if err := waitForBlockCountEquals(outputPath, 1, 4*time.Second); err != nil {
		cancel()
		t.Fatalf("forced recompression did not happen: %v", err)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Run to stop")
	}
}

func waitForCounterAtLeast(path string, target int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			n, convErr := strconv.Atoi(strings.TrimSpace(string(data)))
			if convErr == nil && n >= target {
				return nil
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return context.DeadlineExceeded
}

func waitForRecordCountAtLeast(path string, target int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		records, err := storage.ReadAll(path)
		if err == nil && len(records) >= target {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return context.DeadlineExceeded
}

func waitForBlockCountEquals(path string, expected int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		count, err := blockCountForSignalTest(path)
		if err == nil && count == expected {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return context.DeadlineExceeded
}

func blockCountForSignalTest(path string) (int, error) {
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
