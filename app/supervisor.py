from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

from .updater import atomic_json, git


def main():
    root = Path(__file__).resolve().parents[1]
    state_file = Path(os.getenv("DATABASE_PATH", "data/proxy.db")).resolve().parent / "update-state.json"
    grace = max(float(os.getenv("UPDATES_GRACE_SECONDS", "600")),
                float(os.getenv("STREAM_TOTAL_TIMEOUT", "600")))
    port = os.getenv("PORT", "8000")
    stopping = False
    worker = None

    def stop_signal(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop_signal)
    signal.signal(signal.SIGINT, stop_signal)

    def start():
        # A separate process group lets CTRL_BREAK reach only the uvicorn worker.
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        return subprocess.Popen([sys.executable, "-m", "uvicorn", "app.main:create_app", "--factory", "--host", "0.0.0.0",
                                 "--port", port, "--workers", "1", "--timeout-graceful-shutdown", str(int(grace)),
                                 "--no-access-log", "--no-proxy-headers"], cwd=root, creationflags=flags)

    def stop(process):
        if process.poll() is not None:
            return
        if os.name == "nt":
            # terminate() would TerminateProcess the worker and cut in-flight SSE streams;
            # CTRL_BREAK triggers uvicorn's graceful shutdown so drains stay verifiable.
            process.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=grace + 10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)

    def ready(process):
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline and not stopping and process.poll() is None:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=2) as response:
                    if response.status == 200:
                        return True
            except OSError:
                pass
            time.sleep(.5)
        return False

    def validate():
        for args in (("-m", "compileall", "-q", "app"), ("-c", "from app.main import create_app; create_app()")):
            completed = subprocess.run([sys.executable, *args], cwd=root, timeout=30, capture_output=True)
            if completed.returncode:
                raise RuntimeError("Candidate validation failed (output redacted)")

    try:
        worker = start()
        while not stopping:
            if worker.poll() is not None:
                raise RuntimeError("HTTP worker exited; container restart policy may recover")
            if state_file.is_file():
                state = json.loads(state_file.read_text(encoding="utf-8"))
                if state.get("state") == "pending":
                    import re
                    candidate, previous = state.get("candidate", ""), state.get("previous", "")
                    if not all(re.fullmatch(r"[0-9a-f]{40}", value) for value in (candidate, previous)):
                        raise RuntimeError("Invalid pending commits")
                    if git(root, "rev-parse", "HEAD") != previous or git(root, "status", "--porcelain", "--untracked-files=no"):
                        raise RuntimeError("Update baseline changed")
                    state.update(state="draining")
                    atomic_json(state_file, state)
                    stop(worker)
                    try:
                        git(root, "reset", "--hard", candidate)
                        validate()
                        worker = start()
                        if not ready(worker):
                            raise RuntimeError("Candidate unhealthy")
                        state.update(state="applied", commit=candidate, last_error=None)
                    except Exception:
                        stop(worker)
                        git(root, "reset", "--hard", previous)
                        worker = start()
                        if not ready(worker):
                            raise RuntimeError("Previous version unhealthy; manual recovery required")
                        state.update(state="rolled_back", commit=previous, last_error="candidate_failed")
                    atomic_json(state_file, state)
            time.sleep(1)
    finally:
        if worker:
            stop(worker)


if __name__ == "__main__":
    main()
