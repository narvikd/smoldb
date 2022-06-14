package smoldb

import (
	"bytes"
	"compress/gzip"
	"crypto/sha512"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"github.com/narvikd/filekit"
	"io"
)

func hashInput(input interface{}) (string, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	errEncode := enc.Encode(input)
	if errEncode != nil {
		return "", errWrap(errEncode, "couldn't encode")
	}

	h := sha512.New()
	_, errCopy := io.Copy(h, bytes.NewReader(buf.Bytes()))
	if errCopy != nil {
		return "", errCopy
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// errWrap is a drop-in replacement for errors.errWrap (https://github.com/pkg/errors) using STD's fmt.Errorf().
func errWrap(err error, message string) error {
	return fmt.Errorf("%s: %w", message, err)
}

func compress(input []byte) ([]byte, error) {
	var b bytes.Buffer
	gz, errWriter := gzip.NewWriterLevel(&b, 6)
	if errWriter != nil {
		return nil, errWrap(errWriter, "new compressor")
	}
	if _, err := gz.Write(input); err != nil {
		return nil, errWrap(err, "compressing")
	}
	if err := gz.Close(); err != nil {
		return nil, errWrap(err, "closing/flushing compression")
	}
	return b.Bytes(), nil
}

func decompress(input []byte) ([]byte, error) {
	var b bytes.Buffer
	gz, errReader := gzip.NewReader(&b)
	if errReader != nil {
		return nil, errWrap(errReader, "new decompressor")
	}
	if _, err := gz.Read(input); err != nil {
		return nil, errWrap(err, "decompressing")
	}
	if err := gz.Close(); err != nil {
		return nil, errWrap(err, "closing/flushing decompressor")
	}
	return b.Bytes(), nil
}

func readCompressedFile(path string) ([]byte, error) {
	f, errRead := filekit.ReadFile(path)
	if errRead != nil {
		return nil, errRead
	}
	b, errDecompress := decompress(f)
	if errDecompress != nil {
		return nil, errDecompress
	}
	return b, nil
}
