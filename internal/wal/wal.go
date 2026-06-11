package wal

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog"
)

type OperationType string

const (
	PUT OperationType = "PUT"
	DEL OperationType = "DEL"
)

type WAL struct {
	FilePath string
	FileName string
	log      zerolog.Logger
}

type WALEntry struct {
	OperationType OperationType
	Key           string
	Value         string
}

func CreateWal(filename string, filePath string, logger zerolog.Logger) *WAL {
	return &WAL{
		FilePath: filePath,
		FileName: filename,
		log:      logger,
	}
}

func (wal *WAL) CreateFile() error {
	fullPath := filepath.Join(wal.FilePath, wal.FileName)

	_, err := os.Stat(fullPath)
	if err == nil {
		wal.log.Info().Str("path", fullPath).Msg("WAL file already exists, skipping creation")
		return nil
	}
	if !os.IsNotExist(err) {
		wal.log.Error().Err(err).Str("path", fullPath).Msg("failed to stat WAL file")
		return err
	}

	if err := os.MkdirAll(wal.FilePath, 0755); err != nil {
		wal.log.Error().Err(err).Str("path", wal.FilePath).Msg("failed to create WAL directory")
		return err
	}

	f, err := os.Create(fullPath)
	if err != nil {
		wal.log.Error().Err(err).Str("path", fullPath).Msg("failed to create WAL file")
		return err
	}
	defer f.Close()

	wal.log.Info().Str("path", fullPath).Msg("WAL file created")
	return nil
}

func (wal *WAL) fileExists() (string, error) {
	fullPath := filepath.Join(wal.FilePath, wal.FileName)
	_, err := os.Stat(fullPath)
	if err == nil {
		return fullPath, nil
	}
	return "", errors.New("WAL file does not exist, call CreateFile first")
}

func (wal *WAL) Insert(operationType OperationType, key string, value string) error {
	fullPath, err := wal.fileExists()
	if err != nil {
		wal.log.Error().Err(err).Msg("WAL file not found")
		return err
	}

	file, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_APPEND, 0664)
	if err != nil {
		wal.log.Error().Err(err).Str("path", fullPath).Msg("failed to open WAL file")
		return err
	}
	defer file.Close()

	data := string(operationType) + ",key=" + key + ",value=" + value + "\n"
	_, err = file.WriteString(data)
	if err != nil {
		wal.log.Error().Err(err).Str("path", fullPath).Msg("failed to write to WAL file")
		return err
	}

	wal.log.Info().Str("op", string(operationType)).Str("key", key).Msg("WAL entry written")
	return nil
}

type WALReader struct {
	file    *os.File
	scanner *bufio.Scanner
	line    int
	done    bool
}

func (wal *WAL) NewReader() (*WALReader, error) {
	fullPath, err := wal.fileExists()
	if err != nil {
		wal.log.Error().Err(err).Msg("WAL file not found")
		return nil, err
	}

	file, err := os.Open(fullPath)
	if err != nil {
		wal.log.Error().Err(err).Str("path", fullPath).Msg("failed to open WAL file")
		return nil, err
	}

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	return &WALReader{file: file, scanner: scanner}, nil
}

func (r *WALReader) NextBatch(batchSize int) ([]WALEntry, bool, error) {
	if r.done {
		return nil, false, nil
	}

	entries := make([]WALEntry, 0, batchSize)

	for len(entries) < batchSize && r.scanner.Scan() {
		r.line++
		line := r.scanner.Text()
		content := strings.Split(line, ",")
		if len(content) != 3 {
			// malformed line — skip, keep going
			continue
		}
		entries = append(entries, WALEntry{
			OperationType: OperationType(content[0]),
			Key:           strings.TrimPrefix(content[1], "key="),
			Value:         strings.TrimPrefix(content[2], "value="),
		})
	}

	if err := r.scanner.Err(); err != nil {
		return nil, false, err
	}

	// if we got fewer than batchSize, we hit EOF
	if len(entries) < batchSize {
		r.done = true
		return entries, false, nil
	}

	return entries, true, nil
}

func (r *WALReader) Close() error {
	return r.file.Close()
}
