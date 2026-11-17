#!/usr/bin/env python3
"""Item 52(b) probe: how much a streaming aggregate leaves in DuckDB's external file cache.

Generates one Parquet file, serves it over a local HTTP server with range requests (DuckDB does
not cache local files, it does cache files of extension file systems such as httpfs and
hadoopfs), and runs count/min/max/sum(id) and then a wide scan with
enable_external_file_cache on and off, printing the cache size (duckdb_memory()) and the RSS.

Usage: python3 tools/file-cache-probe.py <httpfs.duckdb_extension> [rows] [memory_limit]
Needs the duckdb Python package of the server's DuckDB version (1.5.6). The httpfs build is in
the ci-assets release (duckdb-extensions-v1.5.6.tar.zst).
"""
import os
import re
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import duckdb


def rss_mib():
    """Returns this process's resident set size in MiB."""
    with open('/proc/self/status') as f:
        for line in f:
            if line.startswith('VmRSS'):
                return int(line.split()[1]) >> 10
    return -1


def serve(root):
    """Serves root over HTTP with range requests on a free port; returns the port."""

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _head(self, status, length, extra=None):
            self.send_response(status)
            self.send_header('Content-Length', str(length))
            self.send_header('Accept-Ranges', 'bytes')
            self.send_header('Last-Modified', 'Mon, 16 Nov 2026 00:00:00 GMT')
            for k, v in (extra or {}).items():
                self.send_header(k, v)
            self.end_headers()

        def do_HEAD(self):
            path = os.path.join(root, self.path.lstrip('/'))
            self._head(200, os.path.getsize(path))

        def do_GET(self):
            path = os.path.join(root, self.path.lstrip('/'))
            size = os.path.getsize(path)
            start, end = 0, size - 1
            m = re.match(r'bytes=(\d+)-(\d*)', self.headers.get('Range', ''))
            if m:
                start = int(m.group(1))
                end = int(m.group(2)) if m.group(2) else size - 1
                self._head(206, end - start + 1, {'Content-Range': f'bytes {start}-{end}/{size}'})
            else:
                self._head(200, size)
            left = end - start + 1
            with open(path, 'rb') as f:
                f.seek(start)
                while left > 0:
                    data = f.read(min(left, 1 << 20))
                    if not data:
                        break
                    self.wfile.write(data)
                    left -= len(data)

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server.server_address[1]


def main():
    httpfs = sys.argv[1]
    rows = int(sys.argv[2]) if len(sys.argv) > 2 else 40_000_000
    limit = sys.argv[3] if len(sys.argv) > 3 else '2GB'
    os.environ['NO_PROXY'] = os.environ['no_proxy'] = '127.0.0.1'
    root = tempfile.mkdtemp(prefix='fcprobe')
    duckdb.connect().execute(
        f"COPY (SELECT range AS id, md5(range::varchar) || md5((range * 7)::varchar) AS pad"
        f" FROM range({rows})) TO '{root}/big.parquet' (FORMAT parquet, ROW_GROUP_SIZE 1000000)")
    url = f'http://127.0.0.1:{serve(root)}/big.parquet'
    print(f'file MiB={os.path.getsize(root + "/big.parquet") >> 20} memory_limit={limit}')
    for cache in ('true', 'false'):
        # A fresh process would be cleaner for RSS; one DuckDB per setting is close enough.
        c = duckdb.connect(config={'allow_unsigned_extensions': 'true'})
        c.execute(f"LOAD '{httpfs}'")
        c.execute(f"SET threads = 4; SET memory_limit = '{limit}'; SET enable_external_file_cache = {cache}")
        for name, sql in (('aggregate', 'count(id), min(id), max(id), sum(id)'),
                          ('wide', 'sum(length(pad))')):
            c.execute(f"SELECT {sql} FROM read_parquet('{url}')").fetchall()
            cached = c.execute("SELECT coalesce(sum(memory_usage_bytes), 0) >> 20 FROM duckdb_memory()"
                               " WHERE tag = 'EXTERNAL_FILE_CACHE'").fetchone()[0]
            print(f'cache={cache} query={name} cacheMiB={cached} rssMiB={rss_mib()}')
        c.close()


if __name__ == '__main__':
    main()
