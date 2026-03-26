package daemon

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"location_logger/internal/paths"
	"location_logger/internal/storage"
)

const childEnv = "LOCATION_LOGGER_DAEMON_CHILD"

type Config struct {
	Interval      time.Duration
	BufferSize    int
	OutputPath    string
	CompactAfter  int
	LocationCmd   string
	SampleTimeout time.Duration
	PendingCap    int
	LockPath      string
	PIDPath       string
	LogPath       string
}

func Start(cfg Config) error {
	if os.Getenv(childEnv) != "1" {
		if err := ensureDetached(); err != nil {
			return err
		}
		return nil
	}
	return Run(context.Background(), cfg)
}

func Run(ctx context.Context, cfg Config) error {
	cfg = withDefaults(cfg)
	if err := validateConfig(cfg); err != nil {
		return err
	}

	if err := ensureRuntimeDirs(cfg); err != nil {
		return err
	}

	lockFile, err := acquireDaemonLock(cfg.LockPath)
	if err != nil {
		return err
	}
	defer lockFile.Close()

	if err := writePIDFile(cfg.PIDPath); err != nil {
		return err
	}
	defer os.Remove(cfg.PIDPath)

	logger, err := openDaemonLogger(cfg.LogPath)
	if err != nil {
		return err
	}
	defer logger.Close()

	if err := storage.EnsureLogFile(cfg.OutputPath); err != nil {
		logger.Logf("storage init failed: %v", err)
		return err
	}

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	termSignals := make(chan os.Signal, 1)
	hupSignals := make(chan os.Signal, 1)
	signal.Notify(termSignals, syscall.SIGINT, syscall.SIGTERM)
	signal.Notify(hupSignals, syscall.SIGHUP)
	defer signal.Stop(termSignals)
	defer signal.Stop(hupSignals)

	pending := make([]storage.Record, 0, cfg.BufferSize)
	flushesSinceCompact := 0

	flushPending := func(label string) error {
		if len(pending) == 0 {
			return nil
		}
		if err := storage.AppendBatch(cfg.OutputPath, pending); err != nil {
			logger.Logf("append failed (%s): %v", label, err)
			return err
		}
		pending = pending[:0]
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			if err := flushPending("shutdown"); err != nil {
				return err
			}
			return nil
		case <-termSignals:
			if err := flushPending("shutdown-signal"); err != nil {
				return err
			}
			return nil
		case <-hupSignals:
			if err := flushPending("sighup"); err != nil {
				logger.Logf("flush on SIGHUP failed: %v", err)
			}
		case <-ticker.C:
			rec, err := sampleOnce(ctx, cfg.LocationCmd, cfg.SampleTimeout)
			if err != nil {
				logger.Logf("sample failed: %v", err)
				continue
			}

			pending = append(pending, rec)
			if cfg.PendingCap > 0 && len(pending) > cfg.PendingCap {
				drop := len(pending) - cfg.PendingCap
				pending = append(make([]storage.Record, 0, cfg.PendingCap), pending[drop:]...)
				logger.Logf("pending cap reached; dropped %d oldest records", drop)
			}

			if len(pending) < cfg.BufferSize {
				continue
			}

			if err := flushPending("buffer-threshold"); err != nil {
				continue
			}

			flushesSinceCompact++
			if flushesSinceCompact >= cfg.CompactAfter {
				if err := storage.Compact(cfg.OutputPath); err != nil {
					logger.Logf("compact failed: %v", err)
				} else {
					flushesSinceCompact = 0
				}
			}
		}
	}
}

func ensureDetached() error {
	cmd := exec.Command(os.Args[0], os.Args[1:]...)
	cmd.Env = append(os.Environ(), childEnv+"=1")

	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open %s: %w", os.DevNull, err)
	}
	defer devNull.Close()

	cmd.Stdin = devNull
	cmd.Stdout = devNull
	cmd.Stderr = devNull
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start daemon process: %w", err)
	}
	return nil
}

func withDefaults(cfg Config) Config {
	if cfg.Interval <= 0 {
		cfg.Interval = 60 * time.Second
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 20
	}
	if cfg.CompactAfter <= 0 {
		cfg.CompactAfter = 100
	}
	if cfg.LocationCmd == "" {
		cfg.LocationCmd = "termux-location"
	}
	if cfg.SampleTimeout <= 0 {
		cfg.SampleTimeout = 20 * time.Second
	}
	if cfg.PendingCap <= 0 {
		cfg.PendingCap = cfg.BufferSize * 10
	}
	return cfg
}

func validateConfig(cfg Config) error {
	if cfg.Interval <= 0 {
		return errorsf("interval must be greater than zero")
	}
	if cfg.BufferSize <= 0 {
		return errorsf("buffer-size must be greater than zero")
	}
	if cfg.CompactAfter <= 0 {
		return errorsf("compact-after must be greater than zero")
	}
	if cfg.OutputPath == "" {
		return errorsf("output path must not be empty")
	}
	if cfg.LockPath == "" || cfg.PIDPath == "" || cfg.LogPath == "" {
		return errorsf("lock, pid, and log paths must not be empty")
	}
	if cfg.LocationCmd == "" {
		return errorsf("location command must not be empty")
	}
	return nil
}

func ensureRuntimeDirs(cfg Config) error {
	if err := paths.EnsureParentDir(cfg.OutputPath); err != nil {
		return err
	}
	if err := paths.EnsureParentDir(cfg.LockPath); err != nil {
		return err
	}
	if err := paths.EnsureParentDir(cfg.PIDPath); err != nil {
		return err
	}
	if err := paths.EnsureParentDir(cfg.LogPath); err != nil {
		return err
	}
	return nil
}

func writePIDFile(path string) error {
	pid := strconv.Itoa(os.Getpid()) + "\n"
	if err := os.WriteFile(path, []byte(pid), 0o644); err != nil {
		return fmt.Errorf("write pid file %q: %w", path, err)
	}
	return nil
}

func errorsf(message string) error {
	return fmt.Errorf(message)
}
