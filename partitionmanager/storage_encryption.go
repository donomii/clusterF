package partitionmanager

import (
	"io"

	"github.com/donomii/clusterF/storagecrypto"
)

func encodeStoredPair(key []byte, legacy bool, path string, metadata, content []byte) ([]byte, []byte, error) {
	encodedMetadata, err := encodeStored(key, legacy, "metadata:"+path, metadata)
	if err != nil {
		return nil, nil, err
	}
	encodedContent, err := encodeStored(key, legacy, "content:"+path, content)
	return encodedMetadata, encodedContent, err
}

func decodeStoredPair(key []byte, legacy bool, path string, metadata, content []byte) ([]byte, []byte, error) {
	decodedMetadata, err := decodeStored(key, legacy, "metadata:"+path, metadata)
	if err != nil {
		return nil, nil, err
	}
	decodedContent, err := decodeStored(key, legacy, "content:"+path, content)
	return decodedMetadata, decodedContent, err
}

func encodeStored(key []byte, legacy bool, context string, plain []byte) ([]byte, error) {
	if legacy {
		return storagecrypto.LegacyEncode(key, plain)
	}
	return storagecrypto.Encode(key, context, plain)
}

func decodeStored(key []byte, legacy bool, context string, encoded []byte) ([]byte, error) {
	if legacy {
		return storagecrypto.LegacyDecode(key, encoded)
	}
	return storagecrypto.Decode(key, context, encoded)
}

func storedReader(source io.ReadCloser, key []byte, legacy bool, context string) (io.ReadCloser, error) {
	if legacy {
		return storagecrypto.LegacyReader(source, key)
	}
	return storagecrypto.Reader(source, key, context)
}

func encryptStored(destination io.Writer, source io.Reader, key []byte, legacy bool, context string) (int64, error) {
	if legacy {
		return storagecrypto.LegacyEncrypt(destination, source, key)
	}
	return storagecrypto.Encrypt(destination, source, key, context)
}
