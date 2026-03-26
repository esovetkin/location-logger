package daemon

import (
	"fmt"
	"os"
	"sync"
	"time"

	"location_logger/internal/paths"
)

type daemonLogger struct {
	mu   sync.Mutex
	file *os.File
}

func openDaemonLogger(path string) (*daemonLogger, error) {
	if err := paths.EnsureParentDir(path); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open daemon log %q: %w", path, err)
	}

	return &daemonLogger{file: file}, nil
}

func (l *daemonLogger) Logf(format string, args ...any) {
	if l == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	line := fmt.Sprintf(format, args...)
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	_, _ = fmt.Fprintf(l.file, "%s %s\n", ts, line)
	_ = l.file.Sync()
}

func (l *daemonLogger) Close() error {
	if l == nil {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}
