"""
concept2_pipeline.auth
======================
Standalone OAuth2 helper.  Run once to obtain a long-lived Bearer token:

    python -m concept2_pipeline.auth

The token is printed to stdout and written to .env (if it exists) so that
Dagster / docker-compose can pick it up as C2_ACCESS_TOKEN.

The Concept2 Logbook API does not issue refresh tokens in the standard OAuth2
sense — the access token is long-lived (effectively permanent until revoked).
Store it as an environment variable or secret manager entry.
"""

import os
import sys
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv

from requests_oauthlib import OAuth2Session

load_dotenv()


CLIENT_ID         = os.getenv("C2_CLIENT_ID", "")
CLIENT_SECRET     = os.getenv("C2_CLIENT_SECRET", "")
REDIRECT_URI      = "http://localhost:8000/callback"
AUTHORIZATION_URL = "https://log.concept2.com/oauth/authorize"
TOKEN_URL         = "https://log.concept2.com/oauth/access_token"
SCOPE             = "user:read,results:read"


class _CallbackHandler(BaseHTTPRequestHandler):
    auth_code: str | None = None
    error: str | None = None

    def do_GET(self):
        params = parse_qs(urlparse(self.path).query)
        if "code" in params:
            _CallbackHandler.auth_code = params["code"][0]
            self._respond("Authorization successful! You can close this tab.")
        elif "error" in params:
            _CallbackHandler.error = params.get("error_description", ["Unknown"])[0]
            self._respond("Authorization failed — check your terminal.")
        else:
            self._respond("Unexpected callback — check your terminal.")

    def _respond(self, msg: str):
        body = f"<html><body><h2>{msg}</h2></body></html>".encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


def obtain_token() -> str:
    """Run the Authorization Code flow and return the access token."""
    if not CLIENT_ID or not CLIENT_SECRET:
        sys.exit(
            "\n[ERROR] Set C2_CLIENT_ID and C2_CLIENT_SECRET environment variables "
            "before running this script.\n"
        )

    os.environ.setdefault("OAUTHLIB_INSECURE_TRANSPORT", "1")

    parsed = urlparse(REDIRECT_URI)
    port   = parsed.port or 8000

    # Reset class-level state in case obtain_token() is called more than once
    _CallbackHandler.auth_code = None
    _CallbackHandler.error     = None

    # serving_started is set from inside the thread the moment serve_forever()
    # is about to enter its select loop — guaranteeing the socket is accepting.
    serving_started = threading.Event()
    code_received   = threading.Event()

    # Patch the handler so it signals the main thread as soon as a code arrives
    class _SignallingHandler(_CallbackHandler):
        def do_GET(self):
            super().do_GET()
            code_received.set()   # wake the main thread immediately

    server = HTTPServer(("localhost", port), _SignallingHandler)
    # SO_REUSEADDR is already set by HTTPServer; bind now so port is reserved
    # before the browser opens.

    def _serve():
        serving_started.set()     # signal AFTER the server object is ready
        server.serve_forever()    # blocks; processes requests until shutdown()

    thread = threading.Thread(target=_serve, daemon=True)
    thread.start()

    if not serving_started.wait(timeout=5):
        sys.exit("[ERROR] Callback server failed to start within 5 seconds.")

    import webbrowser
    oauth = OAuth2Session(CLIENT_ID, redirect_uri=REDIRECT_URI, scope=SCOPE)
    auth_url, _ = oauth.authorization_url(AUTHORIZATION_URL)

    print(f"\nOpening browser for Concept2 authorization…\n{auth_url}\n")
    webbrowser.open(auth_url)

    # Wait for the callback (up to 120 s), then shut the server down cleanly
    code_received.wait(timeout=120)
    server.shutdown()
    thread.join()

    if _CallbackHandler.error:
        sys.exit(f"[ERROR] OAuth error: {_CallbackHandler.error}")
    if not _CallbackHandler.auth_code:
        sys.exit("[ERROR] No authorization code received within 120 seconds.")

    token_data = oauth.fetch_token(
        TOKEN_URL,
        code=_CallbackHandler.auth_code,
        client_secret=CLIENT_SECRET,
        include_client_id=True,
    )
    return token_data["access_token"]


if __name__ == "__main__":
    token = obtain_token()
    print(f"\n[✓] Access token:\n{token}\n")

    # Optionally patch .env
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    if os.path.exists(env_path):
        lines = open(env_path).readlines()
        new_lines = []
        replaced = False
        for line in lines:
            if line.startswith("C2_ACCESS_TOKEN="):
                new_lines.append(f"C2_ACCESS_TOKEN={token}\n")
                replaced = True
            else:
                new_lines.append(line)
        if not replaced:
            new_lines.append(f"C2_ACCESS_TOKEN={token}\n")
        with open(env_path, "w") as f:
            f.writelines(new_lines)
        print(f"[✓] Token written to .env")
