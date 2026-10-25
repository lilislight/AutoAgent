"""Observe physical SQLite growth externally, without querying either Server."""

import argparse
import json
from pathlib import Path
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directories', nargs='+', type=Path)
    parser.add_argument('--interval', type=float, default=5)
    parser.add_argument('--duration', type=float, default=3700)
    parser.add_argument('--output', required=True, type=Path)
    options = parser.parse_args()
    if options.interval <= 0 or options.duration <= 0:
        parser.error('interval and duration must be positive')
    started = time.monotonic()
    options.output.parent.mkdir(parents=True, exist_ok=True)
    with options.output.open('w') as records:
        while time.monotonic() - started < options.duration:
            for directory in options.directories:
                files = {path.name: path.stat().st_size for path in directory.glob('runtime.db*') if path.is_file()}
                records.write(json.dumps(dict(timestamp_us=time.time_ns() // 1000, case=str(directory),
                    files=files, total_bytes=sum(files.values()), finished=(directory / 'result.json').exists())) + '\n')
            records.flush()
            if all((directory / 'result.json').exists() for directory in options.directories):
                break
            time.sleep(options.interval)


if __name__ == '__main__':
    main()
