package daemon

import (
	"path/filepath"
	"testing"
)

func TestAcquireDaemonLockExclusive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.lock")

	lock1, err := acquireDaemonLock(path)
	if err != nil {
		t.Fatalf("first lock acquisition failed: %v", err)
	}
	defer lock1.Close()

	lock2, err := acquireDaemonLock(path)
	if err == nil {
		lock2.Close()
		t.Fatal("expected second lock acquisition to fail")
	}

	if err := lock1.Close(); err != nil {
		t.Fatalf("closing first lock failed: %v", err)
	}

	lock3, err := acquireDaemonLock(path)
	if err != nil {
		t.Fatalf("third lock acquisition failed after release: %v", err)
	}
	_ = lock3.Close()
}
