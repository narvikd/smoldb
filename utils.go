package smoldb

import (
	"bytes"
	"crypto/sha512"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"github.com/DataDog/zstd"
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
	b, err := zstd.CompressLevel(nil, input, zstd.DefaultCompression)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func decompress(input []byte) ([]byte, error) {
	b, err := zstd.Decompress(nil, input)
	if err != nil {
		return nil, err
	}
	return b, nil
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
