# Media Transcoding

ClusterF includes server-side media transcoding to ensure web compatibility for media files that browsers can't play natively.

## Features

- **Automatic fallback**: When media fails to play, automatically tries transcoded version
- **Streaming support**: Transcoded files support HTTP range requests for seeking
- **Smart caching**: 1GB LRU cache to avoid re-transcoding files
- **Web-optimized formats**: 
  - Video → H.264 MP4 with streaming headers
  - Audio → AAC in MP4 container
- **Progress notifications**: Visual feedback during transcoding

## Requirements

Install ffmpeg on your system:

```bash
# Ubuntu/Debian
sudo apt install ffmpeg

# macOS
brew install ffmpeg

# CentOS/RHEL
sudo yum install ffmpeg
```

## Supported Formats

### Input formats transcoded:
- **Video**: WebM, AVI, MKV, MOV, and other formats not natively supported
- **Audio**: WAV, FLAC, OGG, and other formats

### Output formats:
- **Video**: H.264 MP4 with fragmented headers for streaming
- **Audio**: AAC in MP4 container

## API Endpoints

### Transcode File
```
GET /api/transcode/path/to/file?format=web
```

### Cache Statistics  
```
GET /api/transcode-stats
```
Returns cache usage, entry count, and performance metrics.

## How It Works

1. **Client attempts playback** of original file
2. **On error**, automatically requests transcoded version
3. **Server checks cache** for existing transcoded file
4. **If not cached**, ffmpeg transcodes on-demand
5. **Cached file served** with HTTP range support for seeking
6. **LRU cleanup** removes old files when cache limit reached

## Performance

- **First access**: Transcoding takes time (shows "Transcoding..." notification)
- **Subsequent access**: Instant playback from cache
- **Seeking support**: Fragmented MP4 headers allow immediate seeking
- **Cache efficiency**: 1GB cache holds ~100-200 transcoded videos

## Configuration

Cache size and location can be configured in the cluster initialization:

```go
// Located in: {dataDir}/transcode_cache/
// Default size: 1GB
```

## ffmpeg Command Examples

The transcoder uses these ffmpeg settings:

**Video transcoding:**
```bash
ffmpeg -i input.webm \
  -c:v libx264 -preset fast -crf 23 \
  -c:a aac \
  -movflags frag_keyframe+empty_moov \
  -f mp4 output.mp4
```

**Audio transcoding:**
```bash
ffmpeg -i input.wav \
  -c:a aac -b:a 128k \
  -movflags frag_keyframe+empty_moov \
  -f mp4 output.mp4
```

The `frag_keyframe+empty_moov` flags enable HTTP range requests for seeking without downloading the entire file.
