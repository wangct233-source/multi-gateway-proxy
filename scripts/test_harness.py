#!/usr/bin/env python3
"""Isolated loopback-only fixture with four distinct HTTP proxy listeners; not four public IPs."""
from __future__ import annotations

import argparse
import http.client
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

from mock_upstream import Handler as MockHandler, MockServer


class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_GET(self):
        self.forward()

    def do_POST(self):
        self.forward()

    def forward(self):
        url = urlsplit(self.path)
        if url.scheme != "http" or url.hostname != "127.0.0.1" or url.port != 18080:
            self.send_error(403, "fixture only permits own mock upstream")
            return
        size = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(size) if size else None
        headers = {k: v for k, v in self.headers.items() if k.lower() not in {"proxy-authorization", "connection", "host"}}
        connection = http.client.HTTPConnection("127.0.0.1", 18080, timeout=30)
        try:
            connection.request(self.command, url.path or "/", body=body, headers=headers)
            response = connection.getresponse()
            self.send_response(response.status)
            for key, value in response.getheaders():
                if key.lower() not in {"connection", "transfer-encoding", "content-length", "server", "date"}:
                    self.send_header(key, value)
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            while True:
                chunk = response.read1(4096)
                if not chunk:
                    break
                self.wfile.write(f"{len(chunk):X}\r\n".encode() + chunk + b"\r\n")
                self.wfile.flush()
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
        except (OSError, http.client.HTTPException):
            self.close_connection = True
        finally:
            connection.close()


def main():
    args = argparse.Namespace(delay=.03, slow_delay=1, idle_seconds=90)
    server = MockServer(("127.0.0.1", 18080), MockHandler)
    server.options = args
    servers = [server] + [ThreadingHTTPServer(("127.0.0.1", port), ProxyHandler) for port in range(18101, 18106)]
    for item in servers:
        item.daemon_threads = True
        threading.Thread(target=item.serve_forever, daemon=True).start()
    print("loopback mock + four primary proxies and one own-gateway backup fixture ready", flush=True)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        for item in servers:
            item.shutdown()
            item.server_close()


if __name__ == "__main__":
    main()
