package partitionmanager

import "github.com/donomii/clusterF/storagecrypto"

func encodeStoredPair(key []byte, path string, metadata, content []byte) ([]byte, []byte, error) {
	encodedMetadata, err := storagecrypto.Encode(key, "metadata:"+path, metadata)
	if err != nil {
		return nil, nil, err
	}
	encodedContent, err := storagecrypto.Encode(key, "content:"+path, content)
	return encodedMetadata, encodedContent, err
}

func decodeStoredPair(key []byte, path string, metadata, content []byte) ([]byte, []byte, error) {
	decodedMetadata, err := storagecrypto.Decode(key, "metadata:"+path, metadata)
	if err != nil {
		return nil, nil, err
	}
	decodedContent, err := storagecrypto.Decode(key, "content:"+path, content)
	return decodedMetadata, decodedContent, err
}
