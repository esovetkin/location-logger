package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"location_logger/internal/paths"
)

const blockHeaderSize = 12

func AppendBatch(path string, records []Record) error {
	if len(records) == 0 {
		return nil
	}
	if err := paths.EnsureParentDir(path); err != nil {
		return err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("open log file %q: %w", path, err)
	}
	defer file.Close()

	if err := ensureMagic(file); err != nil {
		return err
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek to end of log file: %w", err)
	}

	compressed, rawLen, crc, err := encodeCompressedBlock(records)
	if err != nil {
		return fmt.Errorf("encode block: %w", err)
	}

	if err := writeBlock(file, compressed, rawLen, crc); err != nil {
		return fmt.Errorf("append block: %w", err)
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync log file: %w", err)
	}

	return nil
}

func EnsureLogFile(path string) error {
	if err := paths.EnsureParentDir(path); err != nil {
		return err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("open log file %q: %w", path, err)
	}
	defer file.Close()

	if err := ensureMagic(file); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync log file %q: %w", path, err)
	}
	return nil
}

func ReadAll(path string) ([]Record, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open log file %q: %w", path, err)
	}
	defer file.Close()

	if err := validateMagic(file); err != nil {
		return nil, err
	}

	var records []Record
	for {
		blockRecords, err := readNextBlock(file)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		records = append(records, blockRecords...)
	}

	return records, nil
}

func WriteAllCompressed(path string, records []Record, batchSize int) error {
	if batchSize <= 0 {
		batchSize = 1
	}

	if err := paths.EnsureParentDir(path); err != nil {
		return err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open output file %q: %w", path, err)
	}
	defer file.Close()

	if _, err := file.Write([]byte(FileMagic)); err != nil {
		return fmt.Errorf("write file magic: %w", err)
	}

	for start := 0; start < len(records); start += batchSize {
		end := start + batchSize
		if end > len(records) {
			end = len(records)
		}

		compressed, rawLen, crc, err := encodeCompressedBlock(records[start:end])
		if err != nil {
			return fmt.Errorf("encode block for records [%d:%d]: %w", start, end, err)
		}
		if err := writeBlock(file, compressed, rawLen, crc); err != nil {
			return fmt.Errorf("write block for records [%d:%d]: %w", start, end, err)
		}
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync output file: %w", err)
	}

	return nil
}

func ensureMagic(file *os.File) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek to start of file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}

	switch {
	case info.Size() == 0:
		if _, err := file.Write([]byte(FileMagic)); err != nil {
			return fmt.Errorf("write file magic: %w", err)
		}
		return nil
	case info.Size() < int64(len(FileMagic)):
		return fmt.Errorf("invalid log file: size %d smaller than magic", info.Size())
	default:
		return validateMagic(file)
	}
}

func validateMagic(file *os.File) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek to start of file: %w", err)
	}

	magic := make([]byte, len(FileMagic))
	if _, err := io.ReadFull(file, magic); err != nil {
		return fmt.Errorf("read file magic: %w", err)
	}

	if string(magic) != FileMagic {
		return fmt.Errorf("%w: expected %q, got %q", ErrInvalidMagic, FileMagic, string(magic))
	}

	return nil
}

func writeBlock(w io.Writer, compressed []byte, rawLen uint32, crc uint32) error {
	if len(compressed) > maxBlockCompressedLen {
		return fmt.Errorf("compressed block size %d exceeds maximum %d", len(compressed), maxBlockCompressedLen)
	}

	header := make([]byte, blockHeaderSize)
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(compressed)))
	binary.LittleEndian.PutUint32(header[4:8], rawLen)
	binary.LittleEndian.PutUint32(header[8:12], crc)

	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("write block header: %w", err)
	}
	if _, err := w.Write(compressed); err != nil {
		return fmt.Errorf("write block payload: %w", err)
	}
	return nil
}

func readNextBlock(r io.Reader) ([]Record, error) {
	header := make([]byte, blockHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("truncated block header: %w", err)
		}
		return nil, fmt.Errorf("read block header: %w", err)
	}

	compressedLen := binary.LittleEndian.Uint32(header[0:4])
	rawLen := binary.LittleEndian.Uint32(header[4:8])
	crc := binary.LittleEndian.Uint32(header[8:12])

	if compressedLen == 0 {
		return nil, errors.New("invalid block: compressed length is zero")
	}
	if compressedLen > maxBlockCompressedLen {
		return nil, fmt.Errorf("compressed block length %d exceeds maximum %d", compressedLen, maxBlockCompressedLen)
	}
	if rawLen > maxBlockRawLen {
		return nil, fmt.Errorf("raw block length %d exceeds maximum %d", rawLen, maxBlockRawLen)
	}

	payload := make([]byte, compressedLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("truncated block payload: %w", err)
		}
		return nil, fmt.Errorf("read block payload: %w", err)
	}

	records, err := decodeCompressedBlock(payload, rawLen, crc)
	if err != nil {
		return nil, fmt.Errorf("decode block: %w", err)
	}

	return records, nil
}
