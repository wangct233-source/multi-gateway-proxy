"""G3/G4 native protocol E2E against the local fixture; loopback only."""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]


async def main():
    with tempfile.TemporaryDirectory(prefix="mgp-native-") as tmp:
        mock_port, app_port = 18092, 18093
        mock = subprocess.Popen([sys.executable, str(ROOT / "scripts" / "mock_upstream.py"),
                                 "--port", str(mock_port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        env = dict(os.environ,
                   PORT=str(app_port), DATABASE_PATH=str(Path(tmp) / "proxy.db"),
                   ADMIN_TOKEN="n" * 32, DATA_TOKENS="data-token-123",
                   CORS_ORIGINS="", TASKS_KILL_SWITCH="true",
                   UPDATES_ENABLED="false",
                   B_UPSTREAM_URL=f"http://127.0.0.1:{mock_port}", B_UPSTREAM_MODE="b-remote",
                   B_EGRESS_PRIMARY="direct://local", B_EGRESS_CHECK_URL=f"http://127.0.0.1:{mock_port}/healthz",
                   B_ACCOUNTS_JSON=json.dumps([{"id": "b-acc", "provider_account_id": "trae-user",
                                                "secret_inline": "trae-jwt-token", "enabled": True, "metadata": {}}]),
                   C_UPSTREAM_URL=f"http://127.0.0.1:{mock_port}/api/anthropic", C_UPSTREAM_MODE="c-anthropic",
                   C_EGRESS_PRIMARY="direct://local", C_EGRESS_CHECK_URL=f"http://127.0.0.1:{mock_port}/healthz",
                   C_ACCOUNTS_JSON=json.dumps([{"id": "c-acc", "provider_account_id": "zai-user",
                                                "secret_inline": "key123.secret456", "enabled": True, "metadata": {}}]))
        log = open(Path(tmp) / "app.log", "w")
        app = subprocess.Popen([sys.executable, "-m", "app.supervisor"], cwd=ROOT, env=env,
                               stdout=log, stderr=log)
        base = f"http://127.0.0.1:{app_port}"
        results = {}
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if app.poll() is not None:
                    raise AssertionError("app_exited: " + (Path(tmp) / "app.log").read_text(errors="replace")[-1500:])
                try:
                    if (await httpx.AsyncClient(timeout=1).get(base + "/healthz")).status_code == 200:
                        break
                except httpx.HTTPError:
                    await asyncio.sleep(.2)
            headers = {"Authorization": "Bearer data-token-123"}
            async with httpx.AsyncClient(timeout=30) as client:
                mock_state = "down"
                try:
                    mock_state = str((await client.get(f"http://127.0.0.1:{mock_port}/healthz", timeout=2)).status_code)
                except httpx.HTTPError:
                    pass
                gw = (await client.get(base + "/api/v1/gateways", headers={"Authorization": "Bearer " + "n" * 32})).json()
                egress = {g["id"]: g["egress"] for g in gw["gateways"]}
                assert mock_state == "200", f"mock not ready: {mock_state}"
                assert egress["b"][0]["healthy"], f"b egress unhealthy: {egress}"
                # G3 流式
                async with client.stream("POST", base + "/gw/b/v1/chat/completions", headers=headers,
                                         json={"model": "doubao-pro", "stream": True,
                                               "messages": [{"role": "user", "content": "你好"}]}) as r:
                    raw = b""
                    async for chunk in r.aiter_bytes():
                        raw += chunk
                text = raw.decode()
                assert r.status_code == 200, (r.status_code, text[:300])
                assert '"content":"你好"' in text and "，世界。" in text, text[:400]
                assert '"finish_reason":"stop"' in text and "data: [DONE]" in text
                assert raw.count("你好".encode()) == 1, "snapshot repeated"
                results["b_stream"] = "ok"
                # G3 非流式
                r = await client.post(base + "/gw/b/v1/chat/completions", headers=headers,
                                      json={"model": "doubao-pro", "messages": [{"role": "user", "content": "你好"}]})
                body = r.json()
                assert r.status_code == 200 and body["choices"][0]["message"]["content"] == "你好，世界。", body
                assert body["usage"]["total_tokens"] == 14
                results["b_nonstream"] = "ok"
                # G4 流式
                async with client.stream("POST", base + "/gw/c/v1/chat/completions", headers=headers,
                                         json={"model": "glm-4.7", "stream": True,
                                               "messages": [{"role": "user", "content": "你好"}]}) as r:
                    raw = b""
                    async for chunk in r.aiter_bytes():
                        raw += chunk
                text = raw.decode()
                assert r.status_code == 200 and '"content":"你好"' in text and "data: [DONE]" in text, text[:400]
                results["c_stream"] = "ok"
                # G4 非流式（带工具）
                r = await client.post(base + "/gw/c/v1/chat/completions", headers=headers,
                                      json={"model": "glm-4.7", "tools": [{"type": "function",
                                                                           "function": {"name": "lookup", "parameters": {}}}],
                                            "messages": [{"role": "user", "content": "查天气"}]})
                body = r.json()
                assert r.status_code == 200, (r.status_code, body)
                msg = body["choices"][0]["message"]
                assert msg["tool_calls"][0]["function"]["name"] == "mock_lookup", body
                assert body["choices"][0]["finish_reason"] == "tool_calls"
                results["c_nonstream_tool"] = "ok"
            print(json.dumps(results))
        finally:
            if os.name == "nt":
                subprocess.run(["taskkill", "/T", "/F", "/PID", str(app.pid)], capture_output=True)
            else:
                app.terminate()
            try:
                app.wait(timeout=15)
            except subprocess.TimeoutExpired:
                app.kill()
            mock.terminate()
            try:
                mock.wait(timeout=5)
            except subprocess.TimeoutExpired:
                mock.kill()
            log.close()


if __name__ == "__main__":
    asyncio.run(main())
