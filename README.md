# location_logger

A Go CLI for logging phone location data from Termux and exporting to CSV.

## Build

```bash
go build -o location-logger ./cmd/location-logger
```

## Commands

### `daemon`

Runs a detached background daemon that calls `termux-location` on an interval.

```bash
./location-logger daemon \
  --interval 60 \
  --buffer-size 20 \
  --output ~/.location_logger/data.bin \
  --compact-after 100 \
  --location-cmd "termux-location"
```

Flags:
- `--interval, -i` seconds between samples (default `60`)
- `--buffer-size, -b` successful samples per append (default `20`)
- `--output, -o` binary output path (default `~/.location_logger/data.bin`)
- `--compact-after, -c` successful batch appends before full compaction (default `100`)
- `--location-cmd` command used to query location (default `termux-location`)

### `export`

Exports binary log data to CSV.

```bash
./location-logger export --input ~/.location_logger/data.bin --output /tmp/locations.csv
```

If `--output/-o` is omitted, CSV is written to stdout.

```bash
./location-logger export --input ~/.location_logger/data.bin > /tmp/locations.csv
```

Flags:
- `--input, -i` binary input path (default `~/.location_logger/data.bin`)
- `--output, -o` CSV output path (optional; stdout fallback)

## Runtime Files

All runtime files are under `~/.location_logger`:
- `data.bin` binary compressed log data
- `daemon.lock` single-daemon lock file
- `daemon.pid` daemon pid file
- `daemon.log` timestamped daemon error log

## Daemon Log Format

`daemon.log` uses UTC RFC3339Nano timestamps.

Example:

```text
2026-03-26T12:34:56.789012345Z sample failed: exit status 1
```

## Quickstart with Remote Termux Location

```bash
./location-logger daemon --location-cmd "ssh jojo termux-location"
./location-logger export --output /tmp/locations.csv
```

## Test

```bash
GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod GOPATH=/tmp/go go test ./...
```
