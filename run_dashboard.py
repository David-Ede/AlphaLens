"""One-click AlphaLens launcher for VS Code Run button usage."""

from __future__ import annotations

import importlib.util
import os
import platform
import socket
import subprocess
import sys
import threading
import time
import webbrowser
from pathlib import Path


def _repo_root() -> Path:
    """Return repository root based on this script location."""
    return Path(__file__).resolve().parent


def _ensure_python_version() -> None:
    """Fail fast when launched with an unsupported interpreter."""
    major_minor = tuple(int(part) for part in platform.python_version_tuple()[:2])
    if major_minor < (3, 12):
        msg = "AlphaLens requires Python 3.12+ for this launcher."
        raise SystemExit(msg)


def _runtime_available() -> bool:
    """Return whether app runtime dependencies appear importable."""
    required_modules = ("dash", "fg.ui.app")
    return all(importlib.util.find_spec(mod) is not None for mod in required_modules)


def _install_runtime(repo_root: Path) -> None:
    """Install editable package when runtime dependencies are missing."""
    if _runtime_available():
        return
    print("Installing AlphaLens runtime dependencies (this can take a few minutes)...")
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-e", "."],
            cwd=repo_root,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        msg = f"Failed to install dependencies (exit code {exc.returncode})."
        raise SystemExit(msg) from exc


def _ensure_env_file(repo_root: Path) -> None:
    """Create .env from .env.example on first run if needed."""
    env_file = repo_root / ".env"
    if env_file.exists():
        return
    example = repo_root / ".env.example"
    if not example.exists():
        print("Warning: .env.example not found; continuing without creating .env.")
        return
    env_file.write_text(example.read_text(encoding="utf-8"), encoding="utf-8")
    print("Created .env from .env.example.")


def _port_is_free(host: str, port: int) -> bool:
    """Check whether host:port is free for binding."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((host, port))
        except OSError:
            return False
    return True


def _find_open_port(host: str, start_port: int = 8050, scan_count: int = 100) -> int:
    """Find a free local TCP port, scanning from start_port with fallback to OS-assigned."""
    for port in range(start_port, start_port + scan_count):
        if _port_is_free(host, port):
            return port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        fallback_port = int(sock.getsockname()[1])
    return fallback_port


def _wait_for_server(host: str, port: int, timeout_s: float = 30.0) -> bool:
    """Poll server socket availability until timeout."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            if sock.connect_ex((host, port)) == 0:
                return True
        time.sleep(0.2)
    return False


def _open_browser_when_ready(host: str, port: int) -> None:
    """Open the browser once the Dash server is accepting connections."""
    url = f"http://{host}:{port}"

    def _worker() -> None:
        if _wait_for_server(host, port):
            webbrowser.open(url, new=2)
            print(f"Opened {url}")
        else:
            print(f"Server did not respond in time. Open {url} manually.")

    threading.Thread(target=_worker, daemon=True).start()


def main() -> None:
    """Bootstrap environment and run Dash without terminal command typing."""
    repo_root = _repo_root()
    os.chdir(repo_root)

    _ensure_python_version()
    _install_runtime(repo_root)
    _ensure_env_file(repo_root)

    from fg.ui.app import run_dashboard

    host = "127.0.0.1"
    port = _find_open_port(host=host, start_port=8050, scan_count=100)
    print(f"Starting AlphaLens on http://{host}:{port}")
    _open_browser_when_ready(host=host, port=port)
    run_dashboard(host=host, port=port, debug=False)


if __name__ == "__main__":
    main()
