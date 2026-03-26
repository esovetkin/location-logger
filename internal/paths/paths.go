package paths

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type RuntimePaths struct {
	AppDir    string
	DataFile  string
	LockFile  string
	PIDFile   string
	DaemonLog string
}

func DefaultRuntimePaths() (RuntimePaths, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return RuntimePaths{}, fmt.Errorf("resolve home directory: %w", err)
	}

	appDir := filepath.Join(home, ".location_logger")
	return RuntimePaths{
		AppDir:    appDir,
		DataFile:  filepath.Join(appDir, "data.bin"),
		LockFile:  filepath.Join(appDir, "daemon.lock"),
		PIDFile:   filepath.Join(appDir, "daemon.pid"),
		DaemonLog: filepath.Join(appDir, "daemon.log"),
	}, nil
}

func Expand(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", nil
	}

	if strings.HasPrefix(path, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home directory: %w", err)
		}

		switch {
		case path == "~":
			path = home
		case strings.HasPrefix(path, "~/"):
			path = filepath.Join(home, path[2:])
		default:
			return "", fmt.Errorf("unsupported home path form: %q", path)
		}
	}

	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve absolute path for %q: %w", path, err)
	}
	return abs, nil
}

func EnsureDir(path string) error {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return fmt.Errorf("create directory %q: %w", path, err)
	}
	return nil
}

func EnsureParentDir(path string) error {
	return EnsureDir(filepath.Dir(path))
}
