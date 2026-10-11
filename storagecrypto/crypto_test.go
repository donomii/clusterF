package storagecrypto

import (
	"bytes"
	"testing"
)

func TestPasswordVerification(t *testing.T) {
	settings, key, err := Create("first password")
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := Unlock("first password", settings)
	if err != nil || !bytes.Equal(reopened, key) {
		t.Fatalf("reopening repository: key matches=%t, error=%v", bytes.Equal(reopened, key), err)
	}
	if _, err := Unlock("different password", settings); err == nil {
		t.Fatal("incorrect password accepted")
	}
	settings.Verification[len(settings.Verification)-1] ^= 1
	if _, err := Unlock("first password", settings); err == nil {
		t.Fatal("damaged verification accepted")
	}
}

func TestAuthenticatedStreams(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	for _, size := range []int{0, 1, chunkSize, 3*chunkSize + 17} {
		plain := bytes.Repeat([]byte{42}, size)
		encoded, err := Encode(key, "content:/file", plain)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := Decode(key, "content:/file", encoded)
		if err != nil || !bytes.Equal(decoded, plain) {
			t.Fatalf("round trip size %d: bytes match=%t, error=%v", size, bytes.Equal(decoded, plain), err)
		}
		second, err := Encode(key, "content:/file", plain)
		if err != nil || bytes.Equal(encoded, second) {
			t.Fatalf("encryption reused ciphertext at size %d: %v", size, err)
		}
		if _, err := Decode(key, "content:/another-file", encoded); err == nil {
			t.Fatalf("ciphertext copied to a different path accepted at size %d", size)
		}
		if _, err := Decode(key, "content:/file", encoded[:len(encoded)-1]); err == nil {
			t.Fatalf("truncated ciphertext accepted at size %d", size)
		}
		encoded[len(encoded)-1] ^= 1
		if _, err := Decode(key, "content:/file", encoded); err == nil {
			t.Fatalf("modified ciphertext accepted at size %d", size)
		}
	}
}

func TestAuthenticatedStreamRejectsSplicedVersions(t *testing.T) {
	key := bytes.Repeat([]byte{3}, 32)
	plain := bytes.Repeat([]byte{9}, chunkSize*2)
	first, err := Encode(key, "content:/file", plain)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Encode(key, "content:/file", plain)
	if err != nil {
		t.Fatal(err)
	}
	firstChunk := len(magic) + 32
	copy(first[firstChunk:], second[firstChunk:])
	if _, err := Decode(key, "content:/file", first); err == nil {
		t.Fatal("chunks from a different file version accepted")
	}
	if _, err := Decode(key, "content:/file", []byte("legacy encrypted content")); err == nil {
		t.Fatal("legacy data accepted as authenticated ciphertext")
	}
}
