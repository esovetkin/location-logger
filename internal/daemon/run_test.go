package daemon

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPIDFileLifecycle(t *testing.T) {
	dir := t.TempDir()

	cfg := Config{
		Interval:      50 * time.Millisecond,
		BufferSize:    2,
		OutputPath:    filepath.Join(dir, "data.bin"),
		CompactAfter:  100,
		LocationCmd:   `printf '%s' '{"latitude":1.0,"longitude":2.0}'`,
		SampleTimeout: 2 * time.Second,
		PendingCap:    20,
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

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Run to stop")
	}

	if _, err := os.Stat(cfg.PIDPath); !os.IsNotExist(err) {
		t.Fatalf("pid file should be removed after shutdown, stat error: %v", err)
	}
}

func waitForFileExists(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return context.DeadlineExceeded
}
