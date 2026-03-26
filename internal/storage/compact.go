package storage

import (
	"fmt"
	"os"
)

func Compact(path string) error {
	records, err := ReadAll(path)
	if err != nil {
		return fmt.Errorf("read for compaction: %w", err)
	}

	tmp := path + ".tmp"
	if err := WriteAllCompressed(tmp, records, 800); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("write compacted file: %w", err)
	}

	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace with compacted file: %w", err)
	}

	return nil
}
