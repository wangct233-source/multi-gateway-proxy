from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from app.config import Config, GatewayConfig
from app.core.concurrency import GatewayGate, AdmissionError
from app.core.streaming import UpstreamResponse
from app.db import Database, Repository
from app.gateways.base import upstream_url, GatewayAdapter
from app.pool.lease_manager import AccountPool
from app.updater import verify_signature


class UnitTests(unittest.IsolatedAsyncioTestCase):
    async def test_queue_full_timeout_cancel_release(self):
        gate = GatewayGate(1, 1, .03)
        await gate.acquire()
        waiting = asyncio.create_task(gate.acquire())
        await asyncio.sleep(.001)
        with self.assertRaises(AdmissionError):
            await gate.acquire()
        with self.assertRaises(AdmissionError):
            await waiting
        self.assertEqual(gate.queued, 0)
        waiter = asyncio.create_task(gate.acquire())
        await asyncio.sleep(.001)
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        await gate.release()
        self.assertEqual(gate.active, 0)
        async with gate.slot():
            self.assertEqual(gate.active, 1)
        other = GatewayGate(1, 0, .01)
        self.assertEqual(other.active, 0)

    async def test_gateway_isolation_database_and_idempotent_release(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"T1": "account-one", "T2": "account-two", "T3": "account-three"}):
            db = Database(Path(directory) / "proxy.db")
            await db.start()
            repo = Repository(db)
            cfgs = {g: GatewayConfig(g, g, g, accounts=[{"id": "same", "provider_account_id": "same", "secret_ref": "env:T1" if g == "b" else "env:T2"}]) for g in ("b", "c")}
            await repo.seed(cfgs)
            await repo.add_account("b", {"id": "two", "provider_account_id": "two", "secret_ref": "env:T3"})
            with self.assertRaises(ValueError):
                await repo.add_account("c", {"id": "reuse", "provider_account_id": "reuse", "secret_ref": "env:T1"})
            first, second = AccountPool("b", repo, 1), AccountPool("c", repo, 1)
            await first.load()
            await second.load()
            lease = await first.acquire(only_account="two")
            self.assertEqual(lease.account["id"], "two")
            self.assertEqual(second.in_flight, {})
            await first.release(lease)
            await first.release(lease)
            self.assertEqual(first.in_flight["two"], 0)
            await db.queue.join()
            rows = await db.rows("SELECT * FROM leases")
            self.assertEqual(rows[0]["state"], "released")
            await db.close()

    async def test_stream_raw_bytes_cleanup(self):
        data = 'data: {"text":"你好"}\n\n: heartbeat\n\ndata: [DONE]\n\n'.encode()
        class Stream(httpx.AsyncByteStream):
            async def __aiter__(self):
                for i in range(0, len(data), 1):
                    yield data[i:i+1]
        response = httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Stream())
        reasons, sent = [], []
        async def cleanup(reason):
            await response.aclose()
            reasons.append(reason)
        async def receive():
            await asyncio.sleep(10)
        async def send(message):
            sent.append(message)
        stream = UpstreamResponse(response, cleanup, 2, .5)
        await stream({}, receive, send)
        self.assertEqual(b''.join(m.get("body", b'') for m in sent), data)
        self.assertEqual(reasons, ["completed"])

    async def test_stream_disconnect_releases(self):
        class Stream(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b'data: one\n\n'
                await asyncio.sleep(10)
        response = httpx.Response(200, stream=Stream())
        reasons = []
        async def cleanup(reason):
            await response.aclose()
            reasons.append(reason)
        async def receive():
            await asyncio.sleep(.01)
            return {"type": "http.disconnect"}
        async def send(message):
            pass
        await UpstreamResponse(response, cleanup, 2, 1)({}, receive, send)
        self.assertEqual(reasons, ["client_disconnect"])

    async def test_stream_slow_client_total_deadline(self):
        response = httpx.Response(200, content=b'hello')
        # A streaming HTTP response, not an already-consumed content fixture.
        class Stream(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b'hello'
        response = httpx.Response(200, stream=Stream())
        reasons = []
        async def cleanup(reason): reasons.append(reason)
        async def receive(): await asyncio.sleep(10)
        async def send(message): await asyncio.sleep(10)
        with self.assertRaises(TimeoutError):
            await UpstreamResponse(response, cleanup, .03, 1)({}, receive, send)
        self.assertEqual(reasons, ["upstream_timeout"])

    def test_prefix_join_and_auth_replacement(self):
        self.assertEqual(upstream_url("https://up.example/v1", "v1/chat/completions"), "https://up.example/v1/chat/completions")
        self.assertEqual(upstream_url("https://up.example/base", "v1/chat/completions"), "https://up.example/base/v1/chat/completions")
        cfg = GatewayConfig("a-cn", "A", "A_CN", upstream_url="https://up.example", mode="openai")
        class Lease:
            token = "upstream-token"
        url, headers, body = GatewayAdapter(cfg).prepare("v1/chat/completions", {"messages": [], "_internal": "secret"}, Lease())
        self.assertEqual(headers["Authorization"], "Bearer upstream-token")
        self.assertNotIn("_internal", body)

    def test_a_mode_identity_headers(self):
        from app.gateways.a_domestic import DomesticAdapter, _derive_id, _jwt_sub
        from app.gateways.a_international import InternationalAdapter
        # 无效 token 时 uid 回退到 provider_account_id。
        cfg = GatewayConfig("a-cn", "A 国内", "A_CN", upstream_url="https://copilot.tencent.com", mode="a")
        class Lease:
            account = {"provider_account_id": "user-123"}
            token = "not-a-jwt"
        url, headers, body = DomesticAdapter(cfg).prepare("v1/chat/completions", {"messages": []}, Lease())
        self.assertEqual(url, "https://copilot.tencent.com/v2/chat/completions")
        self.assertEqual(headers["X-User-Id"], "user-123")
        self.assertEqual(headers["X-Machine-ID"], _derive_id("user-123", "machine"))
        self.assertEqual(headers["X-Session-ID"], _derive_id("user-123", "session"))
        self.assertEqual(headers["X-Domain"], "copilot.tencent.com")
        self.assertEqual(headers["Accept-Language"], "zh-CN")
        self.assertIn("X-CodeBuddy-Request", headers)
        # JWT sub 优先于 provider_account_id。
        claims = {"sub": "jwt-user-9"}
        segment = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=").decode()
        jwt_token = "header." + segment + ".signature"
        self.assertEqual(_jwt_sub(jwt_token), "jwt-user-9")
        class JwtLease:
            account = {"provider_account_id": "ignored"}
            token = jwt_token
        _, h2, _ = DomesticAdapter(cfg).prepare("v1/chat/completions", {"messages": []}, JwtLease())
        self.assertEqual(h2["X-User-Id"], "jwt-user-9")
        self.assertEqual(h2["X-Machine-ID"], _derive_id("jwt-user-9", "machine"))
        # 国际 realm 换域名/UA/语言。
        cfg_i = GatewayConfig("a-intl", "A 国际", "A_INTL", upstream_url="https://www.workbuddy.ai", mode="a")
        _, h3, _ = InternationalAdapter(cfg_i).prepare("v1/chat/completions", {"messages": []}, Lease())
        self.assertEqual(h3["X-Domain"], "www.workbuddy.ai")
        self.assertEqual(h3["Accept-Language"], "en-US")
        self.assertEqual(h3["Origin"], "https://www.workbuddy.ai")
        # 带工具的请求在配对修复移植前必须 501，不能假成功。
        from app.gateways.base import EvidenceError
        with self.assertRaises(EvidenceError):
            DomesticAdapter(cfg).prepare("v1/chat/completions", {"messages": [], "tools": []}, Lease())

    def test_hmac(self):
        body, secret = b'{}', 'test-secret'
        signature = 'sha256=' + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        self.assertTrue(verify_signature(body, signature, secret))
        self.assertFalse(verify_signature(body+b'x', signature, secret))
        self.assertFalse(verify_signature(body, signature, ''))

    async def test_bad_write_does_not_rollback_other_batch_items(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Database(Path(directory) / 'proxy.db')
            await db.start()
            first = asyncio.create_task(db.write("INSERT INTO gateways(id,name) VALUES(?,?)", ('one','one')))
            bad = asyncio.create_task(db.write("INSERT INTO gateways(id,name) VALUES(?,?)", ('one','duplicate')))
            second = asyncio.create_task(db.write("INSERT INTO gateways(id,name) VALUES(?,?)", ('two','two')))
            values = await asyncio.gather(first,bad,second,return_exceptions=True)
            self.assertIsInstance(values[1], Exception)
            self.assertEqual(len(await db.rows('SELECT * FROM gateways')), 2)
            await db.close()

    def test_internal_token_reference_refused(self):
        from app.config import secret_value
        with patch.dict(os.environ, {'ADMIN_TOKEN':'admin-value','ACCOUNT':'admin-value'}, clear=True):
            with self.assertRaises(ValueError): secret_value('env:ADMIN_TOKEN')
            with self.assertRaises(ValueError): secret_value('env:ACCOUNT')

    def test_fail_closed_configuration(self):
        with patch.dict(os.environ, {"ADMIN_TOKEN": "x"*32, "A_CN_EGRESS_PRIMARY": "http://p1.example:8000", "A_INTL_EGRESS_PRIMARY": "http://p1.example:8000"}, clear=True):
            with self.assertRaises(ValueError): Config.from_env()
        with patch.dict(os.environ, {"ADMIN_TOKEN": "x"*32, "SOFT_MEMORY_MB": "nan"}, clear=True):
            with self.assertRaises(ValueError): Config.from_env()

    async def test_account_import_formats_and_inline_lease(self):
        from app.main import _extract_credential
        from app.gateways.a_domestic import _jwt_sub
        # A 扁平格式
        claims = base64.urlsafe_b64encode(json.dumps({"sub": "uid-777"}).encode()).rstrip(b"=").decode()
        jwt_a = f"h.{claims}.s"
        token, uid, meta = _extract_credential({"accessToken": jwt_a, "uid": "uid-777", "realm": "cn"})
        self.assertEqual((token, uid), (jwt_a, "uid-777"))
        self.assertEqual(meta["realm"], "cn")
        # A2 嵌套格式（auth/account）
        token, uid, meta = _extract_credential({"auth": {"accessToken": jwt_a}, "account": {"uid": "u2", "nickname": "小明"}})
        self.assertEqual(token, jwt_a)
        self.assertEqual(uid, "u2")
        self.assertEqual(meta["nickname"], "小明")
        # JWT sub 自动提取
        token, uid, _ = _extract_credential({"accessToken": jwt_a})
        self.assertEqual(uid, "uid-777")
        # B 格式（token/user_id）
        token, uid, _ = _extract_credential({"token": "trae-jwt-x", "user_id": "trae-user-1"})
        self.assertEqual((token, uid), ("trae-jwt-x", "trae-user-1"))
        # C 格式（apiKey+secret 拼接）
        token, uid, meta = _extract_credential({"apiKey": "ak123", "secret": "sk456", "userId": "zu", "provider": "zai"})
        self.assertEqual(token, "ak123.sk456")
        self.assertEqual(meta["provider"], "zai")
        # 裸字符串
        token, uid, _ = _extract_credential("  rawtoken ")
        self.assertEqual(token, "rawtoken")
        # 不认识的格式必须报错
        with self.assertRaises(ValueError):
            _extract_credential({"foo": "bar"})
        # inline 入库 + 租约可用 + 不跨网关复用
        with tempfile.TemporaryDirectory() as directory:
            db = Database(Path(directory) / "proxy.db")
            await db.start()
            repo = Repository(db)
            await repo.seed({"b": GatewayConfig("b", "B", "B"), "c": GatewayConfig("c", "C", "C")})
            await repo.add_account("b", {"id": "ib-1", "provider_account_id": "uid-777",
                                         "secret_inline": jwt_a, "enabled": True, "metadata": {"source": "import"}})
            with self.assertRaises(ValueError):
                await repo.add_account("c", {"id": "ic-1", "provider_account_id": "x",
                                             "secret_inline": jwt_a, "enabled": True, "metadata": {}})
            pool = AccountPool("b", repo, 2)
            await pool.load()
            lease = await pool.acquire()
            self.assertEqual(lease.token, jwt_a)
            self.assertEqual(lease.account["id"], "ib-1")
            # API 输出不得包含 inline 凭据
            public = pool.public_accounts()[0]
            self.assertNotIn("secret_inline", public)
            self.assertEqual(public["source"], "imported")
            await db.close()

    async def test_update_image_mode_state_machine(self):
        from app.updater import Updater
        with tempfile.TemporaryDirectory() as directory:
            state_file = Path(directory) / "update-state.json"
            up = Updater(repo_dir=Path(directory), state_file=state_file, repo_slug="o/r",
                         enabled=True, auto_apply=False)
            current, remote = "a" * 40, "b" * 40
            with patch.object(up, "_current_commit", lambda: current), \
                 patch.object(up, "_remote_head", lambda: (remote, "new feature")):
                available = await up.check()
                self.assertEqual(available["state"], "available")
                self.assertEqual(available["candidate"], remote)
                self.assertEqual(available["candidate_subject"], "new feature")
                # apply() arms the host script via the request file.
                armed = await up.apply()
                self.assertEqual(armed["state"], "applying")
            request = json.loads(up.request_file.read_text(encoding="utf-8"))
            self.assertEqual(request["target"], remote)
            self.assertEqual(request["previous"], current)
            self.assertEqual(request["image"], "ghcr.io/o/r:sha-" + remote)
            self.assertTrue(request["previous_image"])
            # While applying, check() must not re-fetch.
            with patch.object(up, "_remote_head", lambda: (_ for _ in ()).throw(AssertionError("no fetch while applying"))):
                same = await up.check()
            self.assertEqual(same["state"], "applying")
            # Host script reports success via the status file and removes the request.
            up.request_file.unlink()
            (up.status_file).write_text(json.dumps({"state": "applied", "commit": remote}), encoding="utf-8")
            self.assertEqual(up.status()["state"], "applied")
            # Rollback report is surfaced as well.
            (up.status_file).write_text(json.dumps({"state": "rolled_back", "commit": current}), encoding="utf-8")
            self.assertEqual(up.status()["state"], "rolled_back")
            up.status_file.unlink()
            # up_to_date never arms anything.
            up2 = Updater(repo_dir=Path(directory), state_file=state_file, repo_slug="o/r", enabled=True)
            with patch.object(up2, "_current_commit", lambda: current), \
                 patch.object(up2, "_remote_head", lambda: (current, "same")):
                fresh = await up2.check()
                self.assertEqual(fresh["state"], "up_to_date")
                self.assertEqual((await up2.apply())["state"], "up_to_date")
            self.assertFalse(up2.request_file.exists())
            # auto_apply=true arms directly on check.
            up3 = Updater(repo_dir=Path(directory), state_file=state_file, repo_slug="o/r",
                          enabled=True, auto_apply=True)
            with patch.object(up3, "_current_commit", lambda: current), \
                 patch.object(up3, "_remote_head", lambda: (remote, "auto")):
                auto = await up3.check()
            self.assertEqual(auto["state"], "applying")
            # Remote/API failure maps to failed, never to applying.
            up3.request_file.unlink()
            up4 = Updater(repo_dir=Path(directory), state_file=state_file, repo_slug="o/r", enabled=True)
            with patch.object(up4, "_current_commit", lambda: current), \
                 patch.object(up4, "_remote_head", lambda: (_ for _ in ()).throw(httpx.ConnectError("down"))):
                failed = await up4.check()
            self.assertEqual(failed["state"], "failed")
            self.assertEqual(failed["last_error"], "fetch_or_validation_failed")


if __name__ == "__main__":
    unittest.main()
