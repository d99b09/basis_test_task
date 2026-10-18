import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        if self.path != "/webhook":
            self.send_error(404)
            return

        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode(
            "utf-8", errors="replace"
        )
        try:
            body = json.dumps(json.loads(body), ensure_ascii=False)
        except json.JSONDecodeError:
            pass
        print(self.date_time_string(), body, flush=True)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def do_GET(self) -> None:
        self.send_error(404)

    def log_message(self, format: str, *args: object) -> None:
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), WebhookHandler).serve_forever()
