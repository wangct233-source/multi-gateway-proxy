from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import math
import re
import sqlite3
import time
import uuid
from dataclasses import replace

import httpx
import psutil
from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route, WebSocketRoute

from .config import Config
from .core.concurrency import AdmissionError, GatewayGate
from .core.streaming import GeneratorResponse, UpstreamResponse
from .db import Database, Repository
from .egress.pool import EgressPool
from .gateways import b_protocol, c_protocol
from .gateways.base import EvidenceError, GatewayAdapter, error, upstream_url
from .pool.lease_manager import AccountPool
from .tasks.scheduler import TaskGuard

log = logging.getLogger("multi_gateway")
VERSION = "0.1.0"


class Runtime:
    def __init__(self, cfg, global_cfg, repository):
        self.config, self.repository = cfg, repository
        self.gate = GatewayGate(cfg.concurrency, cfg.queue_limit, cfg.queue_timeout)
        self.pool = AccountPool(cfg.id, repository, cfg.account_concurrency, global_cfg.stream_total)
        self.egress = EgressPool(cfg, global_cfg)
        from .gateways.a_domestic import DomesticAdapter
        from .gateways.a_international import InternationalAdapter
        from .gateways.b import BAdapter
        from .gateways.c import CAdapter
        self.adapter = {"a-cn": DomesticAdapter, "a-intl": InternationalAdapter, "b": BAdapter, "c": CAdapter}[cfg.id](cfg)
        self.last_error = None

    def public(self):
        return {"id": self.config.id, "name": self.config.name,
                "status": "ready" if any(e.healthy for e in self.egress.exits) else "paused",
                "concurrency": self.config.concurrency, "effective_concurrency": self.gate.limit,
                "active": self.gate.active, "queued": self.gate.queued, "queue_limit": self.gate.queue_limit,
                "accounts": len(self.pool.accounts), "egress": self.egress.public(),
                "capabilities": self.adapter.capabilities(), "tasks_enabled": self.config.tasks_enabled,
                "last_error": self.last_error or self.egress.last_error}


class State:
    def __init__(self, config):
        self.config = config
        self.db = Database(config.database)
        self.repository = Repository(self.db)
        self.runtimes = {gid: Runtime(cfg, config, self.repository) for gid, cfg in config.gateways.items()}
        self.tasks = TaskGuard(config, self.runtimes)
        from .tasks.checkin import CheckinExecutor
        self.checkin = CheckinExecutor(self.tasks)
        self.rss = 0
        self.memory_pressure = False
        self.background = []
        self.updater = None
        self.draining = False
        self.oauth_logins = {}

    async def start(self):
        await self.db.start()
        await self.repository.seed(self.config.gateways)
        for gid, runtime in self.runtimes.items():
            settings = await self.repository.settings(gid)
            apply_settings(runtime, settings)
            await runtime.pool.load()
        await asyncio.gather(*(r.egress.check() for r in self.runtimes.values()))
        from .updater import Updater
        self.updater = Updater(repo_dir=self.config.repo_dir,
                               state_file=self.config.database.parent / "update-state.json",
                               branch=self.config.updates_branch, repo_slug=self.config.updates_repo,
                               enabled=self.config.updates_enabled, auto_apply=self.config.updates_auto_apply)
        log.warning("所有自动任务处于关闭状态；启动总杀开关始终打开，真实执行需证据复核并显式解锁")
        if "null" in self.config.cors_origins:
            log.warning("允许 Origin:null；仅用于可信本机静态 UI，禁止公开暴露该配置")
        for runtime in self.runtimes.values():
            if runtime.config.primary == "direct://local":
                log.warning("gateway=%s direct://local 仅测试；不证明四个独立公网出口", runtime.config.id)
        self.background = [asyncio.create_task(self._health()), asyncio.create_task(self._memory()),
                           asyncio.create_task(self._updates()), asyncio.create_task(self._retention()),
                           asyncio.create_task(self._tasks())]

    async def close(self):
        self.draining = True
        for task in self.background:
            task.cancel()
        await asyncio.gather(*self.background, return_exceptions=True)
        await asyncio.gather(*(r.egress.close() for r in self.runtimes.values()))
        await self.db.close()

    async def _tasks(self):
        while True:
            await asyncio.sleep(60)
            if not self.tasks.kill_switch:
                for gid in self.runtimes:
                    if self.tasks.preview(gid, "checkin")["allowed"]:
                        try:
                            await self.checkin.execute(gid)
                        except Exception:
                            log.error("gateway=%s task failed (details redacted)", gid)

    async def _health(self):
        while True:
            await asyncio.sleep(self.config.egress_check_seconds)
            await asyncio.gather(*(r.egress.check() for r in self.runtimes.values()))
            for gid, runtime in self.runtimes.items():
                for exit in runtime.egress.exits:
                    self.db.enqueue("UPDATE egress SET healthy=?,checked_at=? WHERE gateway_id=? AND id=?",
                                    (int(exit.healthy), time.time(), gid, exit.role))

    async def _memory(self):
        process = psutil.Process()
        while True:
            self.rss = process.memory_info().rss
            self.memory_pressure = self.rss > self.config.soft_memory_mb * 1048576 * self.config.memory_watermark
            for runtime in self.runtimes.values():
                target = max(1, runtime.config.concurrency // 2) if self.memory_pressure else runtime.config.concurrency
                if runtime.gate.limit != target:
                    await runtime.gate.resize(target, runtime.config.queue_limit, runtime.config.queue_timeout)
            await asyncio.sleep(1)

    async def _updates(self):
        while True:
            await asyncio.sleep(self.config.updates_poll_seconds)
            if self.config.updates_enabled:
                try:
                    await self.updater.check()
                except Exception:
                    log.error("Update polling failed (details redacted)")

    async def _retention(self):
        while True:
            await asyncio.sleep(3600)
            cutoff = time.time() - 7 * 86400
            self.db.enqueue("DELETE FROM request_logs WHERE created_at<?", (cutoff,))
            self.db.enqueue("DELETE FROM leases WHERE state!='active' AND released_at<?", (cutoff,))
            self.db.enqueue("DELETE FROM task_runs WHERE started_at<?", (cutoff,))


def authorized(request, tokens):
    header = request.headers.get("authorization", "")
    supplied = header[7:] if header.startswith("Bearer ") else ""
    return bool(supplied) and any(hmac.compare_digest(supplied.encode(), value.encode()) for value in tokens)


def admin(request):
    return authorized(request, (request.app.state.runtime.config.admin_token,))


async def limited_json(request, limit):
    content = bytearray()
    async for chunk in request.stream():
        content.extend(chunk)
        if len(content) > limit:
            raise ValueError("request_body_too_large")
    return json.loads(content)


async def health(request):
    state = request.app.state.runtime
    return JSONResponse({"status": "ok" if not state.draining and not state.db.last_error else "degraded",
                         "memory_rss_bytes": state.rss, "memory_pressure": state.memory_pressure,
                         "gateways": {gid: r.public()["status"] for gid, r in state.runtimes.items()},
                         "database_queue": state.db.queue.qsize(), "dropped_logs": state.db.dropped_logs},
                        status_code=503 if state.draining or state.db.last_error else 200)


async def proxy(request: Request):
    state = request.app.state.runtime
    if not authorized(request, state.config.data_tokens):
        return error(401, "unauthorized", "Valid data Token required")
    gid, path = request.path_params["gateway_id"], request.path_params["path"]
    runtime = state.runtimes.get(gid)
    if runtime is None:
        return error(404, "gateway_not_found", "Unknown gateway")
    if ".." in path.split("/") or path.startswith("gw/") or path.startswith("/"):
        return error(400, "invalid_path", "Gateway prefix must be stripped exactly once")
    if request.method == "GET" and path == "v1/models":
        return JSONResponse({"object": "list", "data": [{"id": m, "object": "model"} for m in runtime.config.models]})
    native_modes = {"b-remote", "c-anthropic"}
    native_ok = runtime.config.mode in native_modes and getattr(runtime.adapter, "protocol", "") == runtime.config.mode
    if request.method != "POST" or path != "v1/chat/completions" or (
            runtime.config.mode not in {"openai", "a"} and not native_ok):
        return error(501, "evidence_required", "Provider protocol is not implemented/verified in this Python build",
                     missing_evidence=["Authorized current protocol samples", "Python converter regression tests"])
    if state.draining or state.db.last_error:
        return error(503, "draining_or_storage_fault", "Update/shutdown or persistent storage fault")
    if state.memory_pressure:
        return error(429, "memory_pressure", "Memory soft watermark reached; retry later")
    try:
        payload = await limited_json(request, state.config.body_limit)
        if not isinstance(payload, dict):
            raise ValueError("JSON object required")
    except (ValueError, json.JSONDecodeError):
        return error(400, "invalid_request", "Invalid or oversized JSON request")
    exit = await runtime.egress.select()
    if not exit:
        return error(503, "no_healthy_egress", "Gateway paused; primary and own backup unavailable")
    if not runtime.config.upstream_url:
        return error(503, "upstream_not_configured", "Configure authorized upstream URL")
    try:
        await runtime.gate.acquire()
    except AdmissionError as exc:
        return error(429, exc.message, "Gateway queue full or wait timeout")
    lease = None
    response = None
    transferred = False
    started = time.monotonic()
    request_id = uuid.uuid4().hex
    code = 503
    cleanup_done = False

    async def cleanup(reason):
        nonlocal cleanup_done
        if cleanup_done:
            return
        cleanup_done = True
        try:
            if response:
                await response.aclose()
        finally:
            if lease:
                await runtime.pool.release(lease)
            await runtime.gate.release()
            state.db.enqueue("INSERT INTO request_logs(gateway_id,request_id,account_id,egress_id,status_code,duration_ms,streamed,error_class,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                             (gid, request_id, lease.account["id"] if lease else None, exit.role if exit else None, code,
                              (time.monotonic() - started) * 1000, int(bool(payload.get("stream"))),
                              None if reason == "completed" else reason, time.time()))
    try:
        exit = await runtime.egress.select()
        if not exit:
            return error(503, "no_healthy_egress", "Gateway egress became unavailable while queued")
        session_key = request.headers.get("x-session-id")
        if session_key:
            session_key = hashlib.sha256(session_key.encode()).hexdigest()
        lease = await runtime.pool.acquire(session_key)
        if not lease:
            return error(429, "no_account_capacity", "No enabled account with credential and available lease")
        adapter_protocol = getattr(runtime.adapter, "protocol", "")
        if adapter_protocol == "b-remote":
            # G3 两步会话协议：create_session(JSON) → events(SSE) → OpenAI 流。
            url, headers, data = runtime.adapter.session_request(payload, lease)
            async with asyncio.timeout(state.config.connect_timeout + state.config.stream_idle):
                outbound = exit.client.build_request("POST", url, headers=headers, json=data)
                session_response = await exit.client.send(outbound)
            if session_response.status_code >= 400:
                code = session_response.status_code
                await runtime.pool.report(lease, code)
                raw = await session_response.aread()
                await session_response.aclose()
                return error(502, "upstream_session_failed",
                             "Trae session creation failed", upstream_status=code,
                             detail=raw[:300].decode("utf-8", errors="replace"))
            try:
                session_payload = session_response.json()
            except ValueError:
                session_payload = {}
            await session_response.aclose()
            session_id, message_id = b_protocol.parse_session_response(session_payload)
            if not session_id or not message_id:
                raise ValueError("trae session response missing ids")
            events_url, events_headers = runtime.adapter.events_request(lease, session_id, message_id)
            async with asyncio.timeout(state.config.connect_timeout + state.config.stream_idle):
                outbound = exit.client.build_request("GET", events_url, headers=events_headers)
                events_response = await exit.client.send(outbound, stream=True)
            code = events_response.status_code
            await runtime.pool.report(lease, code)
            if events_response.status_code >= 400:
                raw = await events_response.aread()
                await events_response.aclose()
                return error(502, "upstream_events_failed", "Trae event stream failed", upstream_status=code,
                             detail=raw[:300].decode("utf-8", errors="replace"))
            source = b_protocol.parse_event_frames(events_response)
            model = str(payload.get("model") or "auto")
            if not payload.get("stream"):
                # 非流式：网关侧消费事件流（有界）合成 OpenAI JSON。
                try:
                    async with asyncio.timeout(state.config.stream_total):
                        raw = await b_protocol.events_to_single(source, model)
                except ValueError as exc:
                    return error(502, "upstream_stream_failed", str(exc)[:200])
                finally:
                    await events_response.aclose()
                await cleanup("completed")
                return Response(raw, media_type="application/json")
            openai_stream = b_protocol.events_to_openai(source, model)
            transformed = GeneratorResponse(openai_stream, cleanup, state.config.stream_total, state.config.stream_idle)
            transferred = True
            return transformed
        url, headers, data = runtime.adapter.prepare(path, payload, lease)
        for attempt in range(2):
            try:
                async with asyncio.timeout(state.config.connect_timeout + state.config.stream_idle):
                    outbound = exit.client.build_request("POST", url, headers=headers, json=data)
                    response = await exit.client.send(outbound, stream=True)
                break
            except (httpx.ConnectError, httpx.ConnectTimeout):
                await runtime.egress.fail(exit)
                alternate = await runtime.egress.select()
                if attempt == 1 or not alternate:
                    return error(503, "egress_connect_failed", "Own egress unavailable")
                exit = alternate
        code = response.status_code
        await runtime.pool.report(lease, code)
        if adapter_protocol == "c-anthropic":
            # G4：Anthropic 请求/响应双向转换；流式逐事件翻译，不整包缓冲。
            model = str(payload.get("model") or "")
            if response.status_code >= 400:
                raw = await response.aread()
                await response.aclose()
                await cleanup("upstream_error")
                return Response(content=raw, status_code=code,
                                media_type=response.headers.get("content-type", "application/json"))
            if payload.get("stream"):
                stream = c_protocol.anthropic_sse_to_openai(response.aiter_raw(), model)
                transformed = GeneratorResponse(stream, cleanup, state.config.stream_total, state.config.stream_idle)
                transferred = True
                return transformed
            raw = await response.aread()
            await response.aclose()
            if len(raw) > state.config.body_limit:
                raise ValueError("anthropic response too large")
            try:
                openai_body = c_protocol.anthropic_to_openai(json.loads(raw), model)
            except ValueError:
                raise ValueError("invalid anthropic response")
            await cleanup("completed")
            return JSONResponse(openai_body)
        transformed = UpstreamResponse(response, cleanup, state.config.stream_total, state.config.stream_idle)
        transferred = True
        return transformed
    except EvidenceError as exc:
        code = 501
        return error(501, "evidence_required", "Adapter capability unavailable", missing_evidence=exc.evidence)
    except ValueError:
        code = 400
        return error(400, "invalid_request", "Adapter rejected request")
    except (httpx.HTTPError, TimeoutError, OSError):
        code = 502
        runtime.last_error = "upstream_transport_error"
        return error(502, "upstream_transport_error", "Upstream failed; request not replayed after write")
    finally:
        if not transferred:
            cleanup_task = asyncio.create_task(cleanup("request_rejected"))
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError:
                await cleanup_task
                raise


async def websocket(socket):
    state = socket.app.state.runtime
    from starlette.responses import JSONResponse
    if not authorized(socket, state.config.data_tokens):
        status, message = 401, {"error": {"code": "unauthorized"}}
    else:
        status, message = 501, {"error": {"code": "evidence_required", "message": "Reviewed upstream WebSocket protocol required"}}
    if "websocket.http.response" in socket.scope.get("extensions", {}):
        await socket.send_denial_response(JSONResponse(message, status_code=status))
    else:
        await socket.close(code=1008, reason="evidence_required")


SETTINGS_KEYS = {"concurrency", "queue_limit", "queue_timeout", "tasks_enabled", "task_window_start", "task_window_end", "task_daily_limit"}


def apply_settings(runtime, values):
    if not set(values) <= SETTINGS_KEYS:
        raise ValueError("Unknown setting")
    for key in ("concurrency", "queue_limit", "task_daily_limit"):
        if key in values and (type(values[key]) is not int or values[key] < (0 if key == "queue_limit" else 1)):
            raise ValueError("Invalid numeric setting")
    if "queue_timeout" in values and (type(values["queue_timeout"]) not in {int, float} or not math.isfinite(values["queue_timeout"]) or values["queue_timeout"] <= 0):
        raise ValueError("Invalid queue timeout")
    if "tasks_enabled" in values and type(values["tasks_enabled"]) is not bool:
        raise ValueError("tasks_enabled must be boolean")
    for key in ("task_window_start", "task_window_end"):
        if key in values and not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", str(values[key])):
            raise ValueError("Invalid time window")
    cfg = replace(runtime.config, **values)
    if cfg.concurrency > cfg.connections:
        raise ValueError("Increase HTTP_MAX_CONNECTIONS env before concurrency")
    for key, value in values.items():
        setattr(runtime.config, key, value)
    runtime.gate.limit = cfg.concurrency
    runtime.gate.queue_limit = cfg.queue_limit
    runtime.gate.timeout = cfg.queue_timeout


async def management(request):
    state = request.app.state.runtime
    if not admin(request):
        return error(401, "unauthorized", "Admin Token required")
    gid = request.path_params.get("gateway_id")
    runtime = state.runtimes.get(gid) if gid else None
    if gid and runtime is None:
        return error(404, "gateway_not_found", "Unknown gateway")
    resource = request.path_params.get("resource", "") or ("accounts" if request.path_params.get("account_id") else "")
    aid = request.path_params.get("account_id")
    if not gid:
        return JSONResponse({"gateways": [r.public() for r in state.runtimes.values()]})
    if resource == "capabilities":
        return JSONResponse({"capabilities": runtime.adapter.capabilities()})
    if resource == "egress":
        return JSONResponse({"egress": runtime.egress.public()})
    if resource == "tasks":
        rows = await state.db.rows("SELECT * FROM task_runs WHERE gateway_id=? ORDER BY id DESC LIMIT 100", (gid,))
        return JSONResponse({"runs": rows})
    if resource == "settings":
        if request.method == "PATCH":
            try:
                values = await limited_json(request, 8192)
                if not isinstance(values, dict):
                    raise ValueError()
                apply_settings(runtime, values)
                await state.repository.set_settings(gid, values)
                await runtime.gate.resize(runtime.gate.limit, runtime.gate.queue_limit, runtime.gate.timeout)
            except (ValueError, TypeError):
                return error(400, "invalid_setting", "Invalid setting or concurrency exceeds HTTP pool")
        return JSONResponse({key: getattr(runtime.config, key) for key in SETTINGS_KEYS})
    if resource == "accounts":
        if request.method == "POST":
            try:
                item = await limited_json(request, 8192)
                if not isinstance(item, dict):
                    raise ValueError()
                await state.repository.add_account(gid, item)
                await runtime.pool.load()
            except (ValueError, TypeError, sqlite3.IntegrityError):
                return error(400, "invalid_account", "Account duplicate or invalid env reference")
        elif request.method == "PATCH":
            try:
                item = await limited_json(request, 8192)
                if not aid or type(item.get("enabled")) is not bool:
                    raise ValueError()
                await state.db.write("UPDATE accounts SET enabled=? WHERE gateway_id=? AND id=?", (int(item["enabled"]), gid, aid))
                await runtime.pool.load()
            except (ValueError, AttributeError):
                return error(400, "invalid_account", "enabled must be boolean")
        return JSONResponse({"accounts": runtime.pool.public_accounts()})
    return error(404, "not_found", "Unknown admin resource")


def _extract_credential(item):
    """把 A/A2/B/C 四种账号池格式统一提取为 (token, uid, meta)。"""
    from .gateways.a_domestic import _jwt_sub
    if isinstance(item, str):
        token = item.strip()
        return token, _jwt_sub(token) or token[:8], {}
    if not isinstance(item, dict):
        raise ValueError("unsupported account item")
    meta = {}
    # A/A2 格式：扁平 accessToken 或嵌套 {auth:{accessToken}, account:{uid,nickname}}。
    auth = item.get("auth") if isinstance(item.get("auth"), dict) else item
    token = str(auth.get("accessToken") or auth.get("access_token") or item.get("token") or "")
    if token:
        uid = str((item.get("account") or {}).get("uid") or item.get("uid") or item.get("user_id")
                  or _jwt_sub(token) or token[:8])
        nickname = (item.get("account") or {}).get("nickname") or item.get("nickname")
        if nickname:
            meta["nickname"] = str(nickname)[:64]
        if item.get("realm"):
            meta["realm"] = str(item["realm"])[:16]
        expires = item.get("expiresAt") or (auth.get("expiresAt") if auth is not item else None)
        if expires:
            meta["expires_at"] = expires
        return token, uid, meta
    # C（Zcode）格式：{apiKey, secret?, userId?}；Z.AI 需要 apiKey.secret 拼接。
    api_key = str(item.get("apiKey") or item.get("api_key") or "")
    if api_key:
        secret = str(item.get("secret") or "")
        token = f"{api_key}.{secret}" if secret else api_key
        uid = str(item.get("userId") or item.get("user_id") or api_key[:8])
        if item.get("provider"):
            meta["provider"] = str(item["provider"])[:32]
        return token, uid, meta
    raise ValueError("no recognized credential field (accessToken/token/apiKey)")


async def account_import(request):
    state = request.app.state.runtime
    if not admin(request):
        return error(401, "unauthorized", "Admin Token required")
    gid = request.path_params["gateway_id"]
    runtime = state.runtimes.get(gid)
    if runtime is None:
        return error(404, "gateway_not_found", "Unknown gateway")
    try:
        body = await limited_json(request, 2097152)
        items = body.get("items") if isinstance(body, dict) else body
        if not isinstance(items, list) or not items or len(items) > 100:
            raise ValueError()
    except (ValueError, json.JSONDecodeError):
        return error(400, "invalid_request", "Provide items as a JSON array (<=100)")
    imported, skipped, failed = 0, 0, []
    for index, item in enumerate(items):
        try:
            token, uid, meta = _extract_credential(item)
            meta.setdefault("source", "import")
            await state.repository.add_account(gid, {
                "id": f"imported-{uid[:24]}-{index}", "provider_account_id": uid[:256],
                "secret_inline": token, "enabled": True, "metadata": meta})
            imported += 1
        except ValueError as exc:
            skipped += 1
            failed.append({"index": index, "reason": str(exc)[:120]})
    await runtime.pool.load()
    return JSONResponse({"gateway_id": gid, "imported": imported, "skipped": skipped, "errors": failed[:20]})


async def account_login_link(request):
    state = request.app.state.runtime
    if not admin(request):
        return error(401, "unauthorized", "Admin Token required")
    gid = request.path_params["gateway_id"]
    runtime = state.runtimes.get(gid)
    if gid not in {"a-cn", "a-intl"} or runtime is None or runtime.config.mode != "a":
        return error(501, "evidence_required", "Browser login link only available for CodeBuddy (a mode)")
    exit = await runtime.egress.select()
    if not exit:
        return error(503, "no_healthy_egress", "Gateway paused")
    from .gateways.a_domestic import REALMS
    cfg = REALMS["cn" if gid == "a-cn" else "intl"]
    headers = {"Content-Type": "application/json", "Accept": "application/json, text/plain, */*",
               "User-Agent": cfg["user_agent"], "Origin": cfg["origin"], "Referer": cfg["origin"] + "/"}
    try:
        response = await exit.client.post(
            upstream_url(runtime.config.upstream_url, "v2/plugin/auth/state") + "?platform=CLI",
            json={}, headers=headers, timeout=30)
        data = (response.json() or {}).get("data") or {}
        login_state, auth_url = data.get("state"), data.get("authUrl")
    except (httpx.HTTPError, ValueError):
        return error(502, "login_start_failed", "Upstream auth/state failed")
    if not login_state or not auth_url:
        return error(502, "login_start_failed", "Upstream returned no state/authUrl")
    state.oauth_logins[login_state] = {"gateway": gid, "created": time.time()}
    return JSONResponse({"state": login_state, "auth_url": auth_url, "expires_in": 600})


async def account_login_status(request):
    state = request.app.state.runtime
    if not admin(request):
        return error(401, "unauthorized", "Admin Token required")
    gid = request.path_params["gateway_id"]
    runtime = state.runtimes.get(gid)
    login_state = request.query_params.get("state", "")
    info = state.oauth_logins.get(login_state)
    if runtime is None or not login_state or not info or info["gateway"] != gid:
        return error(404, "not_found", "Unknown login state")
    if time.time() - info["created"] > 600:
        state.oauth_logins.pop(login_state, None)
        return JSONResponse({"status": "expired"})
    exit = await runtime.egress.select()
    if not exit:
        return error(503, "no_healthy_egress", "Gateway paused")
    from .gateways.a_domestic import REALMS, _jwt_sub
    cfg = REALMS["cn" if gid == "a-cn" else "intl"]
    headers = {"Accept": "application/json, text/plain, */*", "User-Agent": cfg["user_agent"],
               "Origin": cfg["origin"], "Referer": cfg["origin"] + "/"}
    try:
        response = await exit.client.get(
            upstream_url(runtime.config.upstream_url, "v2/plugin/auth/token") + "?state=" + login_state,
            headers=headers, timeout=30)
        payload = response.json() or {}
    except (httpx.HTTPError, ValueError):
        return JSONResponse({"status": "pending"})
    code = payload.get("code")
    if code == 11217:
        return JSONResponse({"status": "pending"})
    if code != 0:
        return JSONResponse({"status": "error", "message": f"code={code} msg={payload.get('msg')}"})
    data = payload.get("data") or {}
    token = data.get("accessToken")
    if not token:
        return JSONResponse({"status": "pending"})
    uid = _jwt_sub(token) or token[:8]
    try:
        await state.repository.add_account(gid, {
            "id": f"oauth-{uid[:24]}", "provider_account_id": uid, "secret_inline": token,
            "enabled": True, "metadata": {"source": "oauth", "expires_at": data.get("expiresAt") or ""}})
    except ValueError as exc:
        return JSONResponse({"status": "error", "message": str(exc)[:120]})
    state.oauth_logins.pop(login_state, None)
    await runtime.pool.load()
    return JSONResponse({"status": "ok", "account_id": f"oauth-{uid[:24]}"})


async def task_run(request):
    state = request.app.state.runtime
    if not admin(request):
        return error(401, "unauthorized", "Admin Token required")
    gid, kind = request.path_params["gateway_id"], request.path_params["task_type"]
    if gid not in state.runtimes or kind not in {"checkin", "claim", "activity"}:
        return error(404, "not_found", "Unknown task")
    try:
        body = await limited_json(request, 8192)
        preview = state.tasks.preview(gid, kind, body.get("account_id"))
    except (ValueError, AttributeError):
        return error(400, "invalid_request", "JSON object required")
    if body.get("dry_run", True) is not True:
        if kind != "checkin" or state.runtimes[gid].adapter.capabilities()[kind]["status"] != "supported":
            return error(501, "evidence_required", "No reviewed Python task executor enabled", missing_evidence=preview["missing_evidence"])
        if not preview["allowed"]:
            return error(403, "task_blocked", "Task switches or execution window prohibit execution", reasons=preview["reason"])
        return JSONResponse(await state.checkin.execute(gid, body.get("account_id")))
    await state.tasks.record_preview(gid, kind, preview)
    return JSONResponse(preview)


async def kill_switch(request):
    state = request.app.state.runtime
    if not admin(request):
        return error(401, "unauthorized", "Admin Token required")
    if request.method == "POST":
        try:
            body = await limited_json(request, 1024)
            if type(body.get("enabled")) is not bool:
                raise ValueError()
            state.tasks.kill_switch = body["enabled"]
        except (ValueError, AttributeError):
            return error(400, "invalid_request", "enabled must be boolean")
    return JSONResponse({"enabled": state.tasks.kill_switch})


async def update_route(request):
    state = request.app.state.runtime
    action = request.path_params["action"]
    if action in {"webhook", "check", "apply"} and request.method != "POST":
        return error(405, "method_not_allowed", "Use POST for update actions")
    if action == "webhook":
        from .updater import verify_signature
        if not state.config.updates_secret or not state.config.updates_enabled:
            return error(503, "updates_disabled", "Updates not configured")
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > 1048576:
                return error(413, "body_too_large", "Webhook too large")
        if not verify_signature(bytes(body), request.headers.get("x-hub-signature-256", ""), state.config.updates_secret):
            return error(401, "invalid_signature", "Webhook HMAC rejected")
        if request.headers.get("x-github-event") != "push":
            return JSONResponse({"ignored": True})
        try:
            payload = json.loads(body)
            if payload.get("ref") != "refs/heads/" + state.config.updates_branch or payload.get("repository", {}).get("full_name") != state.config.updates_repo:
                return JSONResponse({"ignored": True})
        except (ValueError, AttributeError):
            return error(400, "invalid_request", "Invalid webhook")
        return JSONResponse(await state.updater.check(), status_code=202)
    if not admin(request):
        return error(401, "unauthorized", "Admin Token required")
    if action == "status":
        return JSONResponse(state.updater.status())
    if action == "check":
        return JSONResponse(await state.updater.check())
    if action == "apply":
        return JSONResponse(await state.updater.apply())
    return error(404, "not_found", "Unknown update endpoint")


async def version(request):
    if not admin(request):
        return error(401, "unauthorized", "Admin Token required")
    state = request.app.state.runtime
    return JSONResponse({"version": VERSION, "updates": state.updater.status()})


def create_app(config=None):
    config = config or Config.from_env()
    state = State(config)

    @contextlib.asynccontextmanager
    async def lifespan(app):
        await state.start()
        try:
            yield
        finally:
            await state.close()

    app = Starlette(lifespan=lifespan, routes=[
        Route("/healthz", health),
        Route("/api/v1/gateways", management),
        Route("/api/v1/gateways/{gateway_id}/accounts/login-link", account_login_link, methods=["POST"]),
        Route("/api/v1/gateways/{gateway_id}/accounts/login-status", account_login_status, methods=["GET"]),
        Route("/api/v1/gateways/{gateway_id}/accounts/import", account_import, methods=["POST"]),
        Route("/api/v1/gateways/{gateway_id}/tasks/{task_type}/run", task_run, methods=["POST"]),
        Route("/api/v1/gateways/{gateway_id}/accounts/{account_id}", management, methods=["PATCH"], name="accounts"),
        Route("/api/v1/gateways/{gateway_id}/{resource}", management, methods=["GET", "POST", "PATCH"]),
        Route("/api/v1/tasks/kill-switch", kill_switch, methods=["GET", "POST"]),
        Route("/api/v1/version", version),
        Route("/api/updates/{action}", update_route, methods=["GET", "POST"]),
        Route("/gw/{gateway_id}/{path:path}", proxy, methods=["GET", "POST"]),
        WebSocketRoute("/gw/{gateway_id}/{path:path}", websocket),
    ])
    app.state.runtime = state
    app.add_middleware(CORSMiddleware, allow_origins=config.cors_origins,
                       allow_methods=["GET", "POST", "PATCH"], allow_headers=["Authorization", "Content-Type"],
                       expose_headers=["Retry-After"], allow_credentials=False)
    return app
