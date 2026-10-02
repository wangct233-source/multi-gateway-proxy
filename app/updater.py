from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import re
import subprocess
import time
from pathlib import Path

import httpx


def verify_signature(body: bytes, signature: str, secret: str) -> bool:
    if not secret or not re.fullmatch(r"sha256=[0-9a-f]{64}", signature or ""):
        return False
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


def atomic_json(path: Path, value: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value), encoding="utf-8")
    os.replace(temporary, path)


def git_environment():
    env = os.environ.copy()
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GIT_CONFIG_NOSYSTEM"] = "1"
    key_dir = Path(os.getenv("DEPLOY_KEY_DIR", "/app/runtime/keys"))
    key = key_dir / "backend_deploy"
    hosts = key_dir / "known_hosts"
    if key.is_file() and hosts.is_file():
        # Both paths are operator-owned; clients cannot supply an SSH command.
        import shlex
        env["GIT_SSH_COMMAND"] = f"ssh -i {shlex.quote(str(key))} -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile={shlex.quote(str(hosts))}"
    env.pop("GH_TOKEN", None)
    env.pop("GITHUB_TOKEN", None)
    return env


def git(repo: Path, *arguments):
    result = subprocess.run(["git", "-C", str(repo), *arguments], capture_output=True, text=True,
                            timeout=60, env=git_environment())
    if result.returncode:
        raise RuntimeError("Git operation failed (credential and remote details redacted)")
    return result.stdout.strip()


class Updater:
    """Image-mode updater.

    check() compares the running commit against the remote main branch via the
    public GitHub API. apply() writes a request file into the persistent data
    volume; a host-side script pulls the prebuilt ghcr.io image for that commit,
    recreates the container (docker stop drains in-flight requests via the
    supervisor's SIGTERM handling), health-checks and rolls back on failure.
    Dependencies are baked into the image, so requirements.txt changes never
    block an update.
    """

    REQUEST_FILE = "image-update-request.json"
    STATUS_FILE = "image-update-status.json"

    def __init__(self, repo_dir: Path, state_file: Path, branch="main", repo_slug="", enabled=False,
                 auto_apply=False, flag_dir: Path | None = None):
        self.repo_dir, self.state_file = repo_dir, state_file
        self.branch, self.repo_slug, self.enabled = branch, repo_slug, enabled
        self.auto_apply = auto_apply
        self.flag_dir = flag_dir or state_file.parent
        self.lock = asyncio.Lock()
        self.current = {"enabled": enabled, "state": "idle" if enabled else "disabled", "last_error": None,
                        "repo_slug": repo_slug}
        if branch != "main" or (repo_slug and not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo_slug)):
            raise ValueError("Only trusted GitHub main repository supported")

    @property
    def request_file(self) -> Path:
        return self.flag_dir / self.REQUEST_FILE

    @property
    def status_file(self) -> Path:
        return self.flag_dir / self.STATUS_FILE

    def running_image(self) -> str:
        return os.getenv("MGP_IMAGE") or "multi-gateway-proxy:local"

    def _load(self, path: Path):
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            return None

    def status(self):
        result = dict(self.current)
        host_status = self._load(self.status_file)
        if host_status:
            result.update(host_status)
        if self.request_file.exists():
            request = self._load(self.request_file) or {}
            result.update({"state": "applying", "candidate": request.get("target"),
                           "previous": request.get("previous")})
        elif host_status and host_status.get("state") in {"applied", "rolled_back"}:
            result["state"] = host_status["state"]
        result["enabled"] = self.enabled
        return result

    def _current_commit(self):
        try:
            commit = git(self.repo_dir, "rev-parse", "HEAD")
            return commit if re.fullmatch(r"[0-9a-f]{40}", commit) else None
        except (RuntimeError, OSError, subprocess.TimeoutExpired):
            return None

    def _remote_head(self):
        import httpx
        response = httpx.get(f"https://api.github.com/repos/{self.repo_slug}/commits/{self.branch}",
                             headers={"Accept": "application/vnd.github+json",
                                      "User-Agent": "multi-gateway-proxy-updater"}, timeout=15)
        response.raise_for_status()
        payload = response.json()
        commit = payload.get("sha")
        if not re.fullmatch(r"[0-9a-f]{40}", commit or ""):
            raise RuntimeError("unexpected GitHub API response")
        message = (payload.get("commit") or {}).get("message") or ""
        return commit, message.splitlines()[0][:120]

    def _save_current(self):
        atomic_json(self.state_file, dict(self.current))

    async def check(self):
        if not self.enabled:
            return self.status()
        async with self.lock:
            return await self._check_locked()

    async def _check_locked(self):
        if self.status().get("state") in {"applying"}:
            return self.status()
        try:
            current = self._current_commit()
            if not current:
                raise RuntimeError("running commit unknown")
            commit, subject = await asyncio.to_thread(self._remote_head)
            if commit == current:
                update = {"state": "up_to_date", "commit": current, "last_error": None, "checked_at": time.time()}
            else:
                update = {"state": "available", "previous": current, "candidate": commit,
                          "candidate_subject": subject, "last_error": None, "checked_at": time.time()}
        except (RuntimeError, OSError, httpx.HTTPError):
            update = {"state": "failed", "last_error": "fetch_or_validation_failed", "checked_at": time.time()}
        self.current.update(update)
        self._save_current()
        if update["state"] == "available" and self.auto_apply:
            return await self._arm(update)
        return self.status()

    async def apply(self):
        if not self.enabled:
            return self.status()
        async with self.lock:
            state = self.status().get("state")
            if state == "applying":
                return self.status()
            if state != "available":
                return await self._check_locked()
            return await self._arm(self.current)

    async def _arm(self, update):
        request = {"target": update["candidate"], "previous": update.get("previous"),
                   "previous_image": self.running_image(),
                   "image": f"ghcr.io/{self.repo_slug}:sha-{update['candidate']}",
                   "requested_at": time.time()}
        atomic_json(self.request_file, request)
        self.current.update({"state": "applying", "last_error": None})
        self._save_current()
        return self.status()

    def cleanup(self):
        # Host script removed the request on completion; drop a stale request
        # older than 30 minutes so a dead host never leaves "applying" forever.
        request = self._load(self.request_file)
        if request and time.time() - request.get("requested_at", 0) > 1800:
            self.request_file.unlink(missing_ok=True)
            self.current.update({"state": "failed", "last_error": "update_request_timeout"})