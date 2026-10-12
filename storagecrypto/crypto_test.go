package storagecrypto

import (
	"bytes"
	"encoding/hex"
	"io"
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

func TestLegacyCodecFixture(t *testing.T) {
	key := []byte("legacy-password")
	plain := []byte("existing file bytes")
	fixture, err := hex.DecodeString("091d0e121710431741151a1b0a520615110212")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := LegacyEncode(key, plain)
	if err != nil || !bytes.Equal(encoded, fixture) {
		t.Fatalf("legacy encoding changed: bytes=%x, error=%v", encoded, err)
	}
	decoded, err := LegacyDecode(key, fixture)
	if err != nil || !bytes.Equal(decoded, plain) {
		t.Fatalf("legacy fixture unreadable: bytes=%q, error=%v", decoded, err)
	}
	if _, err := LegacyEncode(nil, plain); err == nil {
		t.Fatal("empty legacy key accepted")
	}
}

func TestLegacyStreamingOffsets(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	encodedBlock, err := hex.DecodeString("b0b1b2b3b4b5b6b7b8b9e1e2e3e4e5e6b0b1b2b3b4b5b6b7b8b9e1e2e3e4e5e6")
	if err != nil {
		t.Fatal(err)
	}
	plain := bytes.Repeat([]byte{0x80}, 3*chunkSize+17)
	expected := bytes.Repeat(encodedBlock, len(plain)/len(encodedBlock)+1)[:len(plain)]
	source := io.MultiReader(bytes.NewReader(plain[:3]), bytes.NewReader(plain[3:65537]), bytes.NewReader(plain[65537:]))
	var encoded bytes.Buffer
	written, err := LegacyEncrypt(&encoded, source, key)
	if err != nil || written != int64(len(plain)) || !bytes.Equal(encoded.Bytes(), expected) {
		t.Fatalf("legacy stream encoding changed: written=%d, bytes match=%t, error=%v", written, bytes.Equal(encoded.Bytes(), expected), err)
	}
	reader, err := LegacyReader(io.NopCloser(bytes.NewReader(expected)), key)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var decoded bytes.Buffer
	buffer := make([]byte, 19)
	for {
		count, err := reader.Read(buffer)
		decoded.Write(buffer[:count])
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(decoded.Bytes(), plain) {
		t.Fatal("legacy read offset changed across reads")
	}
	if _, err := Decode(key, "content:/legacy.bin", expected); err == nil {
		t.Fatal("authenticated decoder accepted legacy bytes with a 32-byte key")
	}
}
