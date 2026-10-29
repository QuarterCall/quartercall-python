import http.server
import json
import threading

import pytest


class _Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.server.seen.append(self.headers)
        self.server.paths.append(self.path)
        route = self.server.routes.get(self.path, (404, b"not found"))
        status, body = route[:2]
        if callable(body):
            body = body()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(route[2] if len(route) == 3 else len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class Site:
    """A real HTTP server on localhost, standing in for data.quartercall.com."""

    def __init__(self, server):
        self._server = server
        self.base_url = f"http://127.0.0.1:{server.server_port}/"

    def publish(self, manifest, status=200):
        self._server.routes["/index/manifest.json"] = (status, json.dumps(manifest).encode())
        return self.base_url

    def serve(self, path, body, status=200, *, content_length=None):
        """Serve bytes, or a callable producing bytes, at a dataset path."""
        self._server.routes[path] = (status, body)
        if content_length is not None:
            self._server.routes[path] += (content_length,)

    @property
    def paths(self):
        return self._server.paths

    @property
    def seen(self):
        """The headers of every request received."""
        return self._server.seen


@pytest.fixture
def site():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    server.routes, server.seen, server.paths = {}, [], []
    threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True).start()
    yield Site(server)
    server.shutdown()
    server.server_close()
