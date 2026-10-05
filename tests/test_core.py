from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import sqlite3
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
        token, uid, meta, refresh = _extract_credential({"accessToken": jwt_a, "uid": "uid-777",
                                                         "realm": "cn", "refreshToken": "rt-1"})
        self.assertEqual((token, uid), (jwt_a, "uid-777"))
        self.assertEqual(refresh, "rt-1")
        self.assertEqual(meta["realm"], "cn")
        # A2 嵌套格式（auth/account）
        token, uid, meta, _ = _extract_credential({"auth": {"accessToken": jwt_a}, "account": {"uid": "u2", "nickname": "小明"}})
        self.assertEqual(token, jwt_a)
        self.assertEqual(uid, "u2")
        self.assertEqual(meta["nickname"], "小明")
        # JWT sub 自动提取
        token, uid, _, _ = _extract_credential({"accessToken": jwt_a})
        self.assertEqual(uid, "uid-777")
        # B 格式（token/user_id）
        token, uid, _, _ = _extract_credential({"token": "trae-jwt-x", "user_id": "trae-user-1"})
        self.assertEqual((token, uid), ("trae-jwt-x", "trae-user-1"))
        # C 格式（apiKey+secret 拼接）
        token, uid, meta, _ = _extract_credential({"apiKey": "ak123", "secret": "sk456", "userId": "zu", "provider": "zai"})
        self.assertEqual(token, "ak123.sk456")
        self.assertEqual(meta["provider"], "zai")
        # 裸字符串
        token, uid, _, _ = _extract_credential("  rawtoken ")
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

    def test_b_protocol_two_step(self):
        from app.gateways import b_protocol
        from app.gateways.b import BAdapter
        cfg = GatewayConfig("b", "B", "B", upstream_url="https://trae-api-cn.mchost.guru/api/remote/v1", mode="b-remote")
        class Lease:
            account = {"provider_account_id": "acc-1"}
            token = "trae-jwt-token"
        adapter = BAdapter(cfg)
        url, headers, body = adapter.session_request({"model": "doubao", "messages": [
            {"role": "system", "content": "你是助手"},
            {"role": "user", "content": "你好"}]}, Lease())
        self.assertEqual(url, "https://trae-api-cn.mchost.guru/api/remote/v1/chat_sessions")
        self.assertTrue(headers["Authorization"].startswith("Cloud-IDE-JWT "))
        self.assertEqual(headers["Origin"], "https://solo.trae.cn")
        initial = body["initial_message"]
        self.assertEqual(initial["agent_type"], "solo_agent_remote")
        query = json.loads(initial["query"])
        self.assertIn("[System]\n你是助手", query[0]["data"]["content"])
        self.assertIn("你好", query[0]["data"]["content"])
        common = json.loads(initial["common_params"])
        self.assertEqual(common["device_id"], b_protocol.device_id_for("trae-jwt-token", "acc-1"))
        events_url, _ = adapter.events_request(Lease(), "sid-1", "mid-1")
        self.assertEqual(events_url, "https://trae-api-cn.mchost.guru/api/remote/v1/chat_sessions/sid-1/events?reply_to_message_id=mid-1")

    async def test_b_event_stream_conversion(self):
        from app.gateways import b_protocol

        async def source():
            yield "heartbeat", {}
            yield "message", {"message": {"content": "你好"}}
            yield "message", {"message": {"content": "你好，世界。"}}  # 累积快照 → 增量
            yield "token_usage", {"usage": {"prompt_tokens": 8, "completion_tokens": 6, "total_tokens": 14}}
            yield "done", {"reason": "stop"}

        chunks = []
        async for piece in b_protocol.events_to_openai(source(), "doubao"):
            chunks.append(piece)
        text = b"".join(chunks).decode()
        self.assertIn("你好", text)
        self.assertIn("，世界。", text)
        self.assertNotIn("你好你好", text.replace('\\n', ''))  # 快照不得重复累积
        self.assertIn('"finish_reason":"stop"', text)
        self.assertIn("data: [DONE]", text)
        self.assertIn('"prompt_tokens":8', text)

    async def test_b_event_stream_incremental_frames_not_dropped(self):
        # 上游若改发增量片段（非累积快照），非前缀帧必须输出而不是丢弃。
        from app.gateways import b_protocol

        async def source():
            yield "message", {"message": {"content": "你好"}}
            yield "message", {"message": {"content": "世界"}}  # 增量片段，非前缀
            yield "done", {"reason": "stop"}

        chunks = []
        async for piece in b_protocol.events_to_openai(source(), "doubao"):
            chunks.append(piece)
        text = b"".join(chunks).decode()
        self.assertIn("你好", text)
        self.assertIn("世界", text)
        self.assertIn("data: [DONE]", text)

    async def test_account_duplicate_same_gateway_is_integrity_error(self):
        # 同网关重复 provider_account_id 触发 UNIQUE 约束；导入处理器据此跳过而非 500。
        from app.main import account_import
        from app.db import Database, Repository
        from app.config import GatewayConfig
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            db = Database(Path(directory) / "proxy.db")
            await db.start()
            repo = Repository(db)
            await repo.seed({"b": GatewayConfig("b", "B", "B")})
            item = {"id": "dup-1", "provider_account_id": "uid-dup",
                    "secret_inline": "tok-1", "enabled": True, "metadata": {}}
            await repo.add_account("b", item)
            with self.assertRaises(sqlite3.IntegrityError):
                await repo.add_account("b", {"id": "dup-2", "provider_account_id": "uid-dup",
                                             "secret_inline": "tok-2", "enabled": True, "metadata": {}})
            await db.close()

    async def test_b_event_frame_parser(self):
        from app.gateways import b_protocol

        class FakeResponse:
            async def aiter_bytes(self):
                yield b'event: message\ndata: {"message":{"content":"\xe4\xbd'  # 跨块 UTF-8
                yield b'\xa0\xe5\xa5\xbd"}}\n\n'  # 好
                yield b"data: [DONE]\n\n"

        events = []
        async for event in b_protocol.parse_event_frames(FakeResponse()):
            events.append(event)
        self.assertEqual(events[0][0], "message")
        self.assertEqual(events[0][1]["message"]["content"], "你好")
        self.assertEqual(events[1], ("done", {}))

    def test_c_protocol_request_conversion(self):
        from app.gateways import c_protocol
        from app.gateways.c import CAdapter
        payload = {"model": "glm-4.7", "messages": [
            {"role": "system", "content": "be brief"},
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": "", "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "lookup", "arguments": '{"q":"x"}'}}]},
            {"role": "tool", "tool_call_id": "call_1", "content": "result"}],
            "max_tokens": 100, "temperature": 0.5}
        body = c_protocol.openai_to_anthropic(payload)
        self.assertEqual(body["system"], "be brief")
        self.assertEqual(body["max_tokens"], 100)
        self.assertEqual(body["messages"][1]["content"][0]["type"], "tool_use")
        self.assertEqual(body["messages"][2]["content"][0]["type"], "tool_result")
        cfg = GatewayConfig("c", "C", "C", upstream_url="https://api.z.ai/api/anthropic", mode="c-anthropic")
        class Lease:
            account = {}
            token = "key123.secret456"
        url, headers, converted = CAdapter(cfg).prepare("v1/chat/completions", payload, Lease())
        self.assertEqual(url, "https://api.z.ai/api/anthropic/v1/messages")
        self.assertEqual(headers["x-api-key"], "key123.secret456")
        self.assertEqual(headers["anthropic-version"], "2023-06-01")

    async def test_c_anthropic_sse_conversion(self):
        from app.gateways import c_protocol

        def frame(event, data):
            return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n".encode("utf-8")

        async def byte_stream():
            for piece in [frame("content_block_delta", {"type": "content_block_delta", "index": 0,
                                                        "delta": {"type": "text_delta", "text": "你好"}}),
                          frame("content_block_start", {"type": "content_block_start", "index": 1,
                                                        "content_block": {"type": "tool_use", "id": "t1", "name": "lookup"}}),
                          frame("content_block_delta", {"type": "content_block_delta", "index": 1,
                                                        "delta": {"type": "input_json_delta", "partial_json": '{"q":'}}),
                          frame("content_block_delta", {"type": "content_block_delta", "index": 1,
                                                        "delta": {"type": "input_json_delta", "partial_json": '"x"}'}}),
                          frame("message_delta", {"type": "message_delta", "delta": {"stop_reason": "tool_use"},
                                                  "usage": {"output_tokens": 6}}),
                          frame("message_stop", {"type": "message_stop"})]:
                yield piece

        chunks = []
        async for piece in c_protocol.anthropic_sse_to_openai(byte_stream(), "glm-4.7"):
            chunks.append(piece)
        text = b"".join(chunks).decode()
        self.assertIn('"content":"你好"', text)
        self.assertIn('"name":"lookup"', text)
        self.assertIn('"finish_reason":"tool_calls"', text)
        self.assertIn("data: [DONE]", text)

    def test_c_anthropic_nonstream_conversion(self):
        from app.gateways import c_protocol
        anthropic = {"id": "msg_1", "type": "message", "role": "assistant", "model": "glm-4.7",
                     "content": [{"type": "text", "text": "你好，世界。"},
                                 {"type": "tool_use", "id": "t1", "name": "lookup", "input": {"q": "x"}}],
                     "stop_reason": "tool_use", "usage": {"input_tokens": 8, "output_tokens": 6}}
        openai = c_protocol.anthropic_to_openai(anthropic, "glm-4.7")
        self.assertEqual(openai["choices"][0]["message"]["content"], "你好，世界。")
        self.assertEqual(openai["choices"][0]["message"]["tool_calls"][0]["function"]["name"], "lookup")
        self.assertEqual(json.loads(openai["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"]), {"q": "x"})
        self.assertEqual(openai["choices"][0]["finish_reason"], "tool_calls")
        self.assertEqual(openai["usage"]["total_tokens"], 14)

    def test_risk_policies_isolated_per_gateway(self):
        from app.risk import policy_for, task_interval_for
        from app.risk.codebuddy import CodebuddyPolicy
        from app.risk.trae import TraePolicy
        from app.risk.zcode import ZcodePolicy
        # 策略按网关隔离，互不串用
        self.assertIsInstance(policy_for("a-cn"), CodebuddyPolicy)
        self.assertIsInstance(policy_for("a-intl"), CodebuddyPolicy)
        self.assertIsInstance(policy_for("b"), TraePolicy)
        self.assertIsInstance(policy_for("c"), ZcodePolicy)
        self.assertEqual(task_interval_for("a-cn"), 45.0)
        self.assertEqual(task_interval_for("b"), 60.0)
        self.assertEqual(task_interval_for("c"), 30.0)
        # CodeBuddy：6004 只冷模型不冷账号；11140 停用；余额冷到次日 04:00
        cb = policy_for("a-cn")
        action = cb.classify(200, "6004", "")
        self.assertEqual(action.model_cooldown, 300)
        self.assertEqual(action.account_cooldown, 0)
        self.assertTrue(policy_for("a-cn").classify(200, "11140", "").disable_account)
        balance = cb.classify(200, "1002", "余额不足")
        self.assertGreater(balance.account_cooldown, 0)
        self.assertLess(balance.account_cooldown, 13 * 3600)  # 次日 04:00 封顶
        # Trae：9074 指数退避；重复触发递增
        tp = policy_for("b")
        first = tp.classify(200, "9074", "")
        second = tp.classify(200, "9074", "")
        self.assertGreaterEqual(first.account_cooldown, 60)
        self.assertGreater(second.account_cooldown, first.account_cooldown)  # 退避递增
        self.assertLessEqual(second.account_cooldown, 3600)  # 封顶 1h
        tp.clear_backoff()
        # Zcode：3012 网关级静默（无独立 IP 的退化）+ 次日 0 点封顶；3009 模型级
        zp = policy_for("c")
        hold = zp.classify(200, "3012", "")
        self.assertEqual(hold.egress_cooldown, hold.account_cooldown + hold.egress_cooldown)  # 只冷出口
        self.assertGreaterEqual(hold.egress_cooldown, 600)
        self.assertLessEqual(hold.egress_cooldown, 24 * 3600)
        model = zp.classify(429, "3009", "", retry_after=120)
        self.assertEqual(model.model_cooldown, 120)
        login = zp.classify(401, "", "")
        self.assertTrue(login.disable_account)
        # 错误提取兼容三种形态
        body = json.dumps({"code": 6004, "message": "rate"}).encode()
        self.assertEqual(CodebuddyPolicy.extract_error(body), ("6004", "rate"))
        openai_body = json.dumps({"error": {"code": "x", "message": "m"}}).encode()
        self.assertEqual(CodebuddyPolicy.extract_error(openai_body), ("x", "m"))
        anthropic_body = json.dumps({"type": "error", "error": {"type": "authentication_error", "message": "bad"}}).encode()
        self.assertEqual(ZcodePolicy.extract_error(anthropic_body), ("authentication_error", "bad"))
        # 策略对象互不共享状态
        self.assertIsNot(policy_for("a-cn"), policy_for("b"))

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
            # apply() must arm in the same call when a masked candidate emerges
            # after the re-check (previously it returned "available" unarmed).
            up4.request_file.unlink(missing_ok=True)
            with patch.object(up4, "_current_commit", lambda: current), \
                 patch.object(up4, "_remote_head", lambda: (remote, "masked")), \
                 patch.object(up4, "running_image", lambda: "ghcr.io/o/r:sha-" + current):
                armed_now = await up4.apply()
            self.assertEqual(armed_now["state"], "applying")
            self.assertEqual(json.loads(up4.request_file.read_text(encoding="utf-8"))["target"], remote)

    async def test_update_stale_host_status_is_ignored(self):
        # 手动回滚改写 MGP_IMAGE 后，旧状态文件不得再遮盖真实状态（云端实测教训）。
        from app.updater import Updater
        with tempfile.TemporaryDirectory() as directory:
            state_file = Path(directory) / "update-state.json"
            up = Updater(repo_dir=Path(directory), state_file=state_file, repo_slug="o/r",
                         enabled=True, auto_apply=False)
            current, remote = "a" * 40, "b" * 40
            running = "ghcr.io/o/r:sha-" + current
            # 状态文件来自一个已不在运行的旧镜像：必须被忽略。
            (up.status_file).write_text(json.dumps({
                "state": "applied", "commit": "c" * 40,
                "image": "ghcr.io/o/r:sha-" + "c" * 40}), encoding="utf-8")
            with patch.object(up, "_current_commit", lambda: current), \
                 patch.object(up, "_remote_head", lambda: (remote, "new feature")), \
                 patch.object(up, "running_image", lambda: running):
                available = await up.check()
                self.assertEqual(available["state"], "available")
                armed = await up.apply()
                self.assertEqual(armed["state"], "applying")
            request = json.loads(up.request_file.read_text(encoding="utf-8"))
            self.assertEqual(request["target"], remote)
            up.request_file.unlink()
            with patch.object(up, "running_image", lambda: running):
                # 状态文件不带 image 字段（旧格式）时保持原行为：仍然采纳。
                (up.status_file).write_text(json.dumps({"state": "applied", "commit": remote}), encoding="utf-8")
                self.assertEqual(up.status()["state"], "applied")
                # 状态文件镜像与运行镜像一致时仍然采纳。
                (up.status_file).write_text(json.dumps({
                    "state": "applied", "commit": remote,
                    "image": running}), encoding="utf-8")
                self.assertEqual(up.status()["state"], "applied")
                # 但新一轮 check 发现更新时，旧 applied 必须被清除，不再遮盖。
                with patch.object(up, "_current_commit", lambda: current), \
                     patch.object(up, "_remote_head", lambda: (remote, "newer")):
                    fresh = await up.check()
                self.assertEqual(fresh["state"], "available")
                self.assertFalse(up.status_file.exists())
                armed = await up.apply()
                self.assertEqual(armed["state"], "applying")
                up.request_file.unlink()

    async def test_admin_auth_password_login_and_lockout(self):
        from app.admin_auth import AdminAuth
        from app.db import Database, Repository
        # Windows 上 SQLite 句柄释放晚于 TemporaryDirectory 清理，忽略清理期报错。
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            db = Database(Path(directory) / "test.db")
            await db.start()
            repo = Repository(db)
            auth = AdminAuth()
            await auth.load(repo)
            # 首次启动播种默认密码 admin。
            self.assertTrue(auth.is_default)
            self.assertTrue(auth.verify("admin"))
            self.assertFalse(auth.verify("wrong"))
            # 重启后仍然有效（admin_auth 表持久化），且默认密码标志从哈希反推不丢失。
            reloaded = AdminAuth()
            await reloaded.load(repo)
            self.assertTrue(reloaded.verify("admin"))
            self.assertTrue(reloaded.is_default)
            # 修改密码：新密码生效、旧密码失效、不再是默认态。
            reloaded.set_password("newpass123")
            await reloaded.save(repo)
            changed = AdminAuth()
            await changed.load(repo)
            self.assertFalse(changed.is_default)
            self.assertTrue(changed.verify("newpass123"))
            self.assertFalse(changed.verify("admin"))
            # 防爆破：同 IP 连续错 5 次锁 60 秒；别的 IP 不受影响；成功即解锁。
            for _ in range(5):
                changed.note_failure("1.2.3.4")
            self.assertTrue(changed.locked("1.2.3.4"))
            self.assertFalse(changed.locked("5.6.7.8"))
            changed.note_success("1.2.3.4")
            self.assertFalse(changed.locked("1.2.3.4"))
            # 失败记录表有上限：海量一次性 IP 不撑爆内存，且锁定条目不被清理。
            changed._fails.clear()
            changed.note_failure("locked-ip")
            changed._fails["locked-ip"][1] = __import__("time").time() + 999
            for i in range(5000):
                changed.note_failure(f"scan-{i}")
            self.assertLessEqual(len(changed._fails), 4096)
            self.assertIn("locked-ip", changed._fails)
            await db.close()


if __name__ == "__main__":
    unittest.main()
