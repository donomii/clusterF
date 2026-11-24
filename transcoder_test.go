package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func transcodeTestAudio(frequency float64) []byte {
	const sampleRate = 8000
	const samples = 800
	var audio bytes.Buffer
	audio.WriteString("RIFF")
	binary.Write(&audio, binary.LittleEndian, uint32(36+samples*2))
	audio.WriteString("WAVEfmt ")
	for _, value := range []uint32{16, 0x00010001, sampleRate, sampleRate * 2, 0x00100002} {
		binary.Write(&audio, binary.LittleEndian, value)
	}
	audio.WriteString("data")
	binary.Write(&audio, binary.LittleEndian, uint32(samples*2))
	for i := 0; i < samples; i++ {
		sample := int16(12000 * math.Sin(2*math.Pi*frequency*float64(i)/sampleRate))
		binary.Write(&audio, binary.LittleEndian, sample)
	}
	return audio.Bytes()
}

func TestTranscodeCacheTracksContent(t *testing.T) {
	transcoder := NewTranscoder(t.TempDir(), 32*1024*1024, log.New(io.Discard, "", 0))
	request := TranscodeRequest{InputPath: "/tone.wav", ContentType: "audio/wav", OutputFormat: "web"}
	firstAudio := transcodeTestAudio(440)
	firstPath, err := transcoder.TranscodeToWeb(context.Background(), bytes.NewReader(firstAudio), request)
	if err != nil {
		t.Fatal(err)
	}
	firstInfo, err := os.Stat(firstPath)
	if err != nil {
		t.Fatal(err)
	}
	cachedPath, err := transcoder.TranscodeToWeb(context.Background(), bytes.NewReader(firstAudio), request)
	if err != nil || cachedPath != firstPath {
		t.Fatalf("identical content: path=%q error=%v, want %q", cachedPath, err, firstPath)
	}
	cachedInfo, err := os.Stat(cachedPath)
	if err != nil || !firstInfo.ModTime().Equal(cachedInfo.ModTime()) {
		t.Fatalf("cached conversion was rewritten: %v", err)
	}
	secondPath, err := transcoder.TranscodeToWeb(context.Background(), bytes.NewReader(transcodeTestAudio(880)), request)
	if err != nil || secondPath == firstPath {
		t.Fatalf("replaced content: path=%q error=%v, previous path=%q", secondPath, err, firstPath)
	}
	firstBytes, err := os.ReadFile(firstPath)
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := os.ReadFile(secondPath)
	if err != nil || bytes.Equal(firstBytes, secondBytes) {
		t.Fatalf("different source audio produced identical cached bytes: %v", err)
	}
}

func TestConcurrentTranscodesShareOneConversion(t *testing.T) {
	var output bytes.Buffer
	transcoder := NewTranscoder(t.TempDir(), 32*1024*1024, log.New(&output, "", 0))
	request := TranscodeRequest{InputPath: "/tone.wav", ContentType: "audio/wav", OutputFormat: "web"}
	audio := transcodeTestAudio(440)
	const readers = 8
	paths := make([]string, readers)
	errors := make([]error, readers)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := range readers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			paths[i], errors[i] = transcoder.TranscodeToWeb(context.Background(), bytes.NewReader(audio), request)
		}()
	}
	close(start)
	workers.Wait()
	for i := range readers {
		if errors[i] != nil || paths[i] == "" || paths[i] != paths[0] {
			t.Fatalf("reader %d: path=%q error=%v, want %q", i, paths[i], errors[i], paths[0])
		}
	}
	if conversions := strings.Count(output.String(), "Starting transcode"); conversions != 1 {
		t.Fatalf("started %d conversions for identical concurrent requests, want 1", conversions)
	}
	if stats := transcoder.GetCacheStats(); stats.TotalEntries != 1 || stats.InProgress != 0 {
		t.Fatalf("completed cache state: %+v", stats)
	}
}

func TestOldTranscodeCacheRemainsAccounted(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "0123456789abcdef.mp4")
	content := []byte("previous cache output")
	if err := os.WriteFile(path, content, 0600); err != nil {
		t.Fatal(err)
	}
	transcoder := NewTranscoder(directory, 1, log.New(io.Discard, "", 0))
	if stats := transcoder.GetCacheStats(); stats.TotalEntries != 1 || stats.TotalSize != len(content) {
		t.Fatalf("old cache contents were not accounted: %+v", stats)
	}
	transcoder.cleanupCache()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("old cache entry was not evicted: %v", err)
	}
}
