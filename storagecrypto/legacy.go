package storagecrypto

import (
	"bytes"
	"fmt"
	"io"
)

func LegacyEncode(key, plain []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("legacy encryption key must not be empty")
	}
	encoded := make([]byte, len(plain))
	for index := range plain {
		encoded[index] = plain[index] ^ key[index%len(key)]
	}
	return encoded, nil
}

func LegacyDecode(key, encoded []byte) ([]byte, error) {
	return LegacyEncode(key, encoded)
}

func LegacyEncrypt(destination io.Writer, source io.Reader, key []byte) (int64, error) {
	reader, err := LegacyReader(io.NopCloser(source), key)
	if err != nil {
		return 0, err
	}
	return io.Copy(destination, reader)
}

func LegacyReader(source io.ReadCloser, key []byte) (io.ReadCloser, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("legacy encryption key must not be empty")
	}
	return &legacyReader{source: source, key: bytes.Clone(key)}, nil
}

type legacyReader struct {
	source   io.ReadCloser
	key      []byte
	position int
}

func (reader *legacyReader) Read(destination []byte) (int, error) {
	count, err := reader.source.Read(destination)
	for index := 0; index < count; index++ {
		destination[index] ^= reader.key[reader.position]
		reader.position = (reader.position + 1) % len(reader.key)
	}
	return count, err
}

func (reader *legacyReader) Close() error {
	return reader.source.Close()
}
