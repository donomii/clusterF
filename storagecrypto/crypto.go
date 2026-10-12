package storagecrypto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
)

const format = "clusterF-aes256gcm-pbkdf2-sha256-v1"
const iterations = 600000
const chunkSize = 64 * 1024
const magic = "CLUSTERF-AEAD-1\n"

type Settings struct {
	Format       string `json:"format"`
	Salt         []byte `json:"salt"`
	Verification []byte `json:"verification"`
}

func Create(password string) (*Settings, []byte, error) {
	if password == "" {
		return nil, nil, fmt.Errorf("encryption password must not be empty")
	}
	settings := &Settings{Format: format, Salt: make([]byte, 32)}
	if _, err := rand.Read(settings.Salt); err != nil {
		return nil, nil, err
	}
	key, err := derive(password, settings)
	if err != nil {
		return nil, nil, err
	}
	settings.Verification, err = Encode(key, "repository-key-verification", []byte(format))
	return settings, key, err
}

func Unlock(password string, settings *Settings) ([]byte, error) {
	if password == "" {
		return nil, fmt.Errorf("this repository requires --encryption-key")
	}
	key, err := derive(password, settings)
	if err != nil {
		return nil, err
	}
	plain, err := Decode(key, "repository-key-verification", settings.Verification)
	if err != nil || !bytes.Equal(plain, []byte(format)) {
		return nil, fmt.Errorf("encryption password is incorrect or repository encryption settings are damaged")
	}
	return key, nil
}

func derive(password string, settings *Settings) ([]byte, error) {
	if settings == nil || settings.Format != format || len(settings.Salt) != 32 {
		return nil, fmt.Errorf("unsupported or invalid repository encryption settings")
	}
	return pbkdf2.Key(sha256.New, password, settings.Salt, iterations, 32)
}

func Encode(key []byte, context string, plain []byte) ([]byte, error) {
	if len(key) == 0 {
		return plain, nil
	}
	var encoded bytes.Buffer
	_, err := Encrypt(&encoded, bytes.NewReader(plain), key, context)
	return encoded.Bytes(), err
}

func Decode(key []byte, context string, encoded []byte) ([]byte, error) {
	if len(key) == 0 {
		return encoded, nil
	}
	reader, err := Reader(io.NopCloser(bytes.NewReader(encoded)), key, context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("expected a derived 32-byte encryption key, received %d bytes", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCMWithRandomNonce(block)
}

func associatedData(streamID []byte, context string, index uint64) []byte {
	data := append([]byte(context), 0)
	data = append(data, streamID...)
	return binary.BigEndian.AppendUint64(data, index)
}

func Encrypt(destination io.Writer, source io.Reader, key []byte, context string) (int64, error) {
	if len(key) == 0 {
		return io.Copy(destination, source)
	}
	aead, err := newAEAD(key)
	if err != nil {
		return 0, err
	}
	streamID := make([]byte, 32)
	if _, err := rand.Read(streamID); err != nil {
		return 0, err
	}
	header := append([]byte(magic), streamID...)
	if n, err := destination.Write(header); err != nil {
		return 0, err
	} else if n != len(header) {
		return 0, io.ErrShortWrite
	}
	buffer := make([]byte, chunkSize)
	var written int64
	for index := uint64(0); ; index++ {
		n, readErr := io.ReadFull(source, buffer)
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			return written, readErr
		}
		sealed := aead.Seal(nil, nil, buffer[:n], associatedData(streamID, context, index))
		frame := binary.BigEndian.AppendUint32(nil, uint32(len(sealed)))
		frame = append(frame, sealed...)
		if count, err := destination.Write(frame); err != nil {
			return written, err
		} else if count != len(frame) {
			return written, io.ErrShortWrite
		}
		written += int64(n)
		if n == 0 {
			return written, nil
		}
	}
}

type decryptReader struct {
	source   io.ReadCloser
	aead     cipher.AEAD
	streamID []byte
	context  string
	index    uint64
	pending  []byte
	finished bool
}

func Reader(source io.ReadCloser, key []byte, context string) (io.ReadCloser, error) {
	if len(key) == 0 {
		return source, nil
	}
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	header := make([]byte, len(magic)+32)
	if _, err := io.ReadFull(source, header); err != nil {
		return nil, fmt.Errorf("reading encrypted %s header: %w", context, err)
	}
	if string(header[:len(magic)]) != magic {
		return nil, fmt.Errorf("expected authenticated encryption format for %s", context)
	}
	return &decryptReader{source: source, aead: aead, streamID: header[len(magic):], context: context}, nil
}

func (reader *decryptReader) Read(destination []byte) (int, error) {
	if len(destination) == 0 {
		return 0, nil
	}
	if len(reader.pending) == 0 {
		if reader.finished {
			return 0, io.EOF
		}
		if err := reader.next(); err != nil {
			return 0, err
		}
	}
	n := copy(destination, reader.pending)
	reader.pending = reader.pending[n:]
	return n, nil
}

func (reader *decryptReader) next() error {
	var length [4]byte
	if _, err := io.ReadFull(reader.source, length[:]); err != nil {
		return fmt.Errorf("encrypted %s is incomplete: %w", reader.context, err)
	}
	size := int(binary.BigEndian.Uint32(length[:]))
	if size < reader.aead.Overhead() || size > chunkSize+reader.aead.Overhead() {
		return fmt.Errorf("invalid encrypted chunk size %d for %s", size, reader.context)
	}
	sealed := make([]byte, size)
	if _, err := io.ReadFull(reader.source, sealed); err != nil {
		return fmt.Errorf("encrypted %s chunk is incomplete: %w", reader.context, err)
	}
	plain, err := reader.aead.Open(nil, nil, sealed, associatedData(reader.streamID, reader.context, reader.index))
	if err != nil {
		return fmt.Errorf("authentication failed for encrypted %s chunk %d: %w", reader.context, reader.index, err)
	}
	reader.index++
	if len(plain) == 0 {
		var trailing [1]byte
		if n, err := io.ReadFull(reader.source, trailing[:]); n != 0 || err != io.EOF {
			return fmt.Errorf("unexpected trailing data after encrypted %s", reader.context)
		}
		reader.finished = true
		return io.EOF
	}
	reader.pending = plain
	return nil
}

func (reader *decryptReader) Close() error {
	return reader.source.Close()
}
