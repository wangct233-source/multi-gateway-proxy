#!/usr/bin/env python3
"""Operator-run, explicit GitHub provisioning; never imports local write credentials into Docker."""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
from pathlib import Path


def run(args, env=None, stdin=None):
    result = subprocess.run(args, input=stdin, text=True, capture_output=True, env=env, timeout=90)
    if result.returncode:
        raise RuntimeError(f"Command failed: {args[0]} {args[1]} (details redacted)")
    return result.stdout.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Create two repositories and read-only deploy key")
    parser.add_argument("--ui-dir", type=Path)
    args = parser.parse_args()
    if not shutil.which("gh"):
        raise SystemExit("Official gh not installed; install GitHub.cli on host and authenticate with browser, never paste token")
    root = Path(__file__).resolve().parents[1]
    ui = (args.ui_dir or root.parent / "multi-gateway-proxy-ui").resolve()
    env = dict(os.environ, GH_HOST="github.com", GH_PROMPT_DISABLED="1")
    run(["gh", "--version"], env)
    run(["gh", "auth", "status", "--hostname", "github.com"], env)
    owner = run(["gh", "api", "--hostname", "github.com", "user", "--jq", ".login"], env)
    if not re.fullmatch(r"[A-Za-z0-9-]+", owner):
        raise SystemExit("Ambiguous GitHub identity")
    names = [os.getenv("BACKEND_REPO_NAME", "multi-gateway-proxy"), os.getenv("UI_REPO_NAME", "multi-gateway-proxy-ui")]
    if any(not re.fullmatch(r"[A-Za-z0-9_.-]+", name) for name in names):
        raise SystemExit("Invalid repository name")
    print(json.dumps({"host": "github.com", "owner": owner, "repositories": names, "apply": args.apply}))
    for name in names:
        check = subprocess.run(["gh", "api", f"repos/{owner}/{name}"], env=env, capture_output=True, text=True)
        if check.returncode == 0:
            raise SystemExit("Name exists; confirm ownership/reuse explicitly before provisioning")
        if "404" not in check.stderr:
            raise SystemExit("Cannot verify repository availability")
    if not args.apply:
        return
    for directory, name, visibility in zip((root, ui), names, ("--private", "--public")):
        if run(["git", "-C", str(directory), "status", "--porcelain"], env):
            raise SystemExit("Commit reviewed files before publishing")
        if run(["git", "-C", str(directory), "symbolic-ref", "--short", "HEAD"], env) != "main":
            raise SystemExit("Expected main branch")
        slug = owner + "/" + name
        scoped = dict(env, GH_REPO="github.com/" + slug)
        run(["gh", "repo", "create", slug, visibility, "--source", str(directory), "--remote", "origin", "--push"], scoped)
        print("created https://github.com/" + slug)
    slug = owner + "/" + names[0]
    scoped = dict(env, GH_REPO="github.com/" + slug)
    keys = root.parent / "runtime" / "keys"
    keys.mkdir(parents=True, exist_ok=True)
    key = keys / "backend_deploy"
    if key.exists():
        raise SystemExit("Key exists; never overwrite a deploy key automatically")
    run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", "multi-gateway-readonly", "-f", str(key)])
    payload = {"title": "multi-gateway-runtime-readonly", "key": key.with_suffix(".pub").read_text().strip(), "read_only": True}
    run(["gh", "api", f"repos/{slug}/keys", "--method", "POST", "--input", "-"], scoped, json.dumps(payload))
    print("Read-only deploy key generated outside repository: " + str(key))
    print("Pin GitHub verified SSH host key in runtime/keys/known_hosts before enabling updates; do not trust ssh-keyscan alone")
    public = os.getenv("PUBLIC_BACKEND_URL", "").rstrip("/")
    secret = os.getenv("UPDATES_WEBHOOK_SECRET", "")
    from urllib.parse import urlsplit
    parsed = urlsplit(public)
    if parsed.scheme == "https" and parsed.hostname not in {None, "localhost", "127.0.0.1"} and not parsed.username and not parsed.query and not parsed.fragment and len(secret) >= 32:
        hook = {"name": "web", "active": True, "events": ["push"], "config": {"url": public + "/api/updates/webhook", "content_type": "json", "secret": secret, "insecure_ssl": "0"}}
        run(["gh", "api", f"repos/{slug}/hooks", "--method", "POST", "--input", "-"], scoped, json.dumps(hook))
        print("Webhook configured; runtime validates refs/heads/main; polling fallback 300s")
    else:
        print("Webhook pending: PUBLIC_BACKEND_URL HTTPS and env secret required; no fictitious callback configured")
    print("Local gh credentials were never mounted, copied or passed to the backend image")


if __name__ == "__main__":
    main()
