"""Trae remote 两步会话协议（G3 / b-remote）。

机制来源（只读参考，未复制代码）：Trae2api-cn src/trae_remote_client.py:808-943,
src/trae_client.py:891-935, src/sse.py:1691-1879。
协议要点：
- 先 POST {base}/chat_sessions 创建会话（JSON），再 GET .../events 流式读私有事件帧。
- 认证 ``Authorization: Cloud-IDE-JWT {token}``。
- 事件内容多为累积快照（非增量），需在网关侧计算文本增量。
- 事件名经 ``event:`` 行或 data.event 字段给出；``data: [DONE]`` 终止。
"""
from __future__ import annotations

import hashlib
import json
import re
import uuid

# 事件归一（trae_remote_client.py:1050-1062 同类映射）
_EVENT_ALIASES = {
    "modelconfig": "model_config", "planitem": "plan_item",
    "responsedone": "done", "response_done": "done",
    "streamdone": "done", "stream_done": "done", "tokenusage": "token_usage",
}
_TEXT_EVENTS = {"message", "assistant_message", "response", "text", "output"}


def normalize_event(name: str) -> str:
    text = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", str(name or "").strip())
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_").lower()
    return _EVENT_ALIASES.get(normalized, normalized)


def device_id_for(token: str, account_id: str) -> str:
    """稳定设备号（trae_client.py:275-279 同款派生，不随请求漂移）。"""
    digest = hashlib.sha256(f"trae{account_id or ''}".encode("utf-8")).hexdigest()
    try:
        return str(int(digest[:32], 16) % 10**16).zfill(16)
    except ValueError:
        return "0" * 16


def _content_to_text(content) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict):
                if str(block.get("type") or "").lower() in {"reasoning", "thinking", "reasoning_text"}:
                    continue
                text = block.get("text") or block.get("content")
                if isinstance(text, str):
                    parts.append(text)
        return "".join(parts)
    return str(content)


def flatten_query(messages: list) -> str:
    """OpenAI 消息 → web remote 的扁平 query JSON（trae_client.py:891-935 同构）。"""
    parts: list[str] = []
    for m in messages or []:
        if not isinstance(m, dict):
            continue
        role = str(m.get("role") or "user").lower()
        content = _content_to_text(m.get("content"))
        if role == "system":
            parts.append(f"[System]\n{content}")
        elif role == "assistant":
            calls = m.get("tool_calls")
            block = content
            if isinstance(calls, list) and calls:
                rendered = []
                for call in calls:
                    if not isinstance(call, dict):
                        continue
                    fn = call.get("function") or {}
                    rendered.append(f"- {fn.get('name') or call.get('name') or 'tool'}({fn.get('arguments') or ''})")
                block = (block + "\n\n" if block else "") + "[Tool Calls]\n" + "\n".join(rendered)
            if block:
                parts.append(f"[Assistant]\n{block}")
        elif role == "tool":
            tool_id = m.get("tool_call_id") or "unknown"
            tool_name = m.get("name") or "tool"
            parts.append(f"[Client Tool Result: {tool_id} {tool_name} status=succeeded]\n{content}")
        else:
            parts.append(content)
    text = "\n\n".join(p for p in parts if p)
    return json.dumps([{"type": "text", "data": {"content": text}}], ensure_ascii=False)


def session_body(payload: dict, lease_token: str, provider_account_id: str) -> dict:
    """构造 chat_sessions 创建请求体（trae_remote_client.py:808-888 精简子集）。"""
    model = str(payload.get("model") or "")
    model_name = model if model and model.lower() not in {"auto", "work", "auto-work", "solo-work"} else ""
    mode = "work" if model.lower() in {"work", "auto-work", "solo-work"} else "code"
    agent_type = "solo_work_remote" if mode == "work" else "solo_agent_remote"
    account_id = str(provider_account_id or "")
    device = device_id_for(lease_token, account_id)
    machine_id = hashlib.sha256("\x1f".join([account_id, device, "traework-linux-probe"]).encode()).hexdigest()
    common = {
        "language": "zh-cn", "app_language": "zh-CN", "quality": "stable",
        "app_version": "1.0.0.1229", "user_identity": "Free", "is_freshman": "0",
        "scope": "marscode-cn", "tenant": "marscode", "region": "cn", "aiRegion": "cn",
        "is_privacy_mode": 0, "privacy_mode": "off", "solo_chat_mode": mode,
        "device_id": device, "machine_id": machine_id,
    }
    initial = {
        "chat_session_id": "",
        "content": [],
        "query": flatten_query(payload.get("messages") or []),
        "model_name": model_name,
        "agent_type": agent_type,
        "agent_id": agent_type,
        "model_selection_strategy": "auto",
        "common_params": json.dumps(common, ensure_ascii=False),
    }
    return {"mode": mode, "environment_id": "default", "initial_message": initial,
            "env": "remote", "auto_create_project": False, "origin": "web"}


def session_headers(token: str, stream: bool, intl: bool = False) -> dict:
    origin = "https://solo.trae.ai" if intl else "https://solo.trae.cn"
    return {
        "Authorization": "Cloud-IDE-JWT " + token,
        "Content-Type": "application/json",
        "X-Trae-Client-Type": "web",
        "X-Preferenced-Language": "en" if intl else "zh-CN",
        "x-user-region": "US" if intl else "CN",
        "Origin": origin,
        "Referer": origin + "/",
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"),
        "Accept": "text/event-stream" if stream else "application/json",
        "Accept-Encoding": "identity",
    }


def parse_session_response(payload: dict) -> tuple[str, str]:
    data = payload.get("data") if isinstance(payload, dict) else None
    data = data if isinstance(data, dict) else (payload if isinstance(payload, dict) else {})
    return str(data.get("chat_session_id") or ""), str(data.get("message_id") or "")


# ── 积分/余额查询（强证据：trae_client.py:536-555,618-638,695-738 实际调用）──
UG_API_HOST = "https://api.trae.cn"
CHECKIN_STATUS_PATH = "trae/api/v2/ug/checkin_credits/status"
ENT_USAGE_PATH = "trae/api/v2/pay/ide_user_ent_usage"


def checkin_headers(token: str, account_id: str) -> dict:
    """签到/积分接口头（build_checkin_headers 同构）。"""
    return {
        "Authorization": "Cloud-IDE-JWT " + token,
        "Content-Type": "application/json",
        "x-device-id": device_id_for(token, account_id),
        "x-device-brand": "trae",
        "x-device-type": "windows",
        "Accept-Encoding": "identity",
    }


def credits_requests(token: str, account_id: str) -> list[tuple[str, dict, dict]]:
    """构造两个查询：(签到状态, 账户积分余额)。返回 [(name, url, headers, body)]。"""
    headers = checkin_headers(token, account_id)
    return [
        ("checkin_status", f"{UG_API_HOST}/{CHECKIN_STATUS_PATH}", headers, {}),
        ("credits", f"{UG_API_HOST}/{ENT_USAGE_PATH}", headers, {"require_usage": True, "req_source": 1}),
    ]


def parse_b_credits(checkin_payload, usage_payload) -> dict:
    """聚合签到状态与积分余额（parse_account_credits 同构）。"""
    out: dict = {}
    if isinstance(checkin_payload, dict):
        data = checkin_payload.get("data") if isinstance(checkin_payload.get("data"), dict) else checkin_payload
        if "checked_in" in data or "credits" in data:
            out["checked_in"] = bool(data.get("checked_in"))
            try:
                out["checkin_credits"] = float(data.get("credits") or 0)
            except (TypeError, ValueError):
                pass
    if not isinstance(usage_payload, dict):
        return out
    total = used = 0.0
    packs = usage_payload.get("user_entitlement_pack_list") or []
    for pack in packs:
        if not isinstance(pack, dict):
            continue
        base_info = pack.get("entitlement_base_info") or {}
        quota = base_info.get("quota") or {}
        try:
            total += float(quota.get("credits_limit") or 0)
        except (TypeError, ValueError):
            pass
        usage = pack.get("usage") or {}
        try:
            used += float(usage.get("credits_amount") or 0)
        except (TypeError, ValueError):
            pass
    if total:
        out["total_limit"] = total
        out["used"] = used
        out["remaining"] = max(0.0, total - used)
    out["unlimited"] = bool(usage_payload.get("is_credits_billing")) if "is_credits_billing" in usage_payload else out.get("unlimited", False)
    return out


def _message_text(data: dict) -> str:
    """提取可见文本（sse.py:1691-1722 同构）。"""
    if not isinstance(data, dict):
        return ""
    for key in ("message", "agent_message", "assistant_message"):
        nested = data.get(key)
        if isinstance(nested, dict):
            data = nested
            break
    content = data.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return _content_to_text(content)
    for key in ("text", "response", "answer", "output"):
        value = data.get(key)
        if isinstance(value, str):
            return value
    return ""


async def parse_event_frames(response):
    """httpx 流式响应 → (event_name, data_dict) 迭代器（trae_remote_client.py:966-1002 改进版）。

    字节级缓冲后再按行解码：跨传输块的 UTF-8 序列不会被截断成替换符。
    ``data: [DONE]`` 以 ("done", {}) 终止。
    """
    event_name = None
    buffer = b""
    async for raw in response.aiter_bytes():
        buffer += raw
        while b"\n" in buffer:
            line_bytes, buffer = buffer.split(b"\n", 1)
            line = line_bytes.decode("utf-8", errors="replace").rstrip("\r")
            if line.startswith(":"):
                continue
            if line.startswith("event:"):
                event_name = line[6:].strip()
                continue
            if line == "":
                event_name = None
                continue
            if not line.startswith("data:"):
                continue
            payload = line[5:].strip()
            if payload == "[DONE]":
                yield "done", {}
                return
            try:
                data = json.loads(payload)
            except ValueError:
                data = {"_raw": payload}
            if not isinstance(data, dict):
                data = {"value": data}
            yield event_name or str(data.get("event") or "message"), data
            event_name = None


def _finish_reason(data: dict) -> str:
    raw = str((data or {}).get("reason") or (data or {}).get("finish_reason") or "").lower()
    if "length" in raw or "token" in raw:
        return "length"
    if "tool" in raw:
        return "tool_calls"
    return "stop"


def _openai_chunk(chunk_id: str, model: str, delta: dict, finish=None, usage=None) -> bytes:
    body = {"id": chunk_id, "object": "chat.completion.chunk", "created": 0, "model": model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
    if usage is not None:
        body["usage"] = usage
    return ("data: " + json.dumps(body, ensure_ascii=False, separators=(",", ":")) + "\n\n").encode("utf-8")


async def events_to_openai(source, model: str):
    """Trae 事件帧迭代器 → OpenAI chat.completion.chunk SSE 字节流。

    source 产出 (event_name, data_dict) 二元组；累积快照在网关侧计算增量，
    不整包缓冲。heartbeat 以 SSE 注释帧保活。
    """
    chunk_id = "chatcmpl-" + uuid.uuid4().hex
    snapshot = ""
    usage = None
    finish = "stop"
    saw_done = False
    try:
        yield _openai_chunk(chunk_id, model, {"role": "assistant"})
        async for name, data in source:
            event = normalize_event(name)
            if event in {"heartbeat", "keepalive", "ping"}:
                yield b": heartbeat\n\n"
                continue
            if event == "error":
                payload = data if isinstance(data, dict) else {}
                message = str(payload.get("message") or payload.get("error") or payload.get("detail") or "upstream error")
                code = str(payload.get("code") or "")
                error_body = {"error": {"message": f"trae upstream error{' ' + code if code else ''}: {message}",
                                        "type": "upstream_error", "code": "upstream_error"}}
                yield f"data: {json.dumps(error_body, ensure_ascii=False)}\n\n".encode("utf-8")
                yield b"data: [DONE]\n\n"
                return
            if event == "token_usage":
                raw = data.get("usage") if isinstance(data, dict) else None
                raw = raw if isinstance(raw, dict) else (data if isinstance(data, dict) else {})
                usage = {"prompt_tokens": int(raw.get("prompt_tokens") or raw.get("input_tokens") or 0),
                         "completion_tokens": int(raw.get("completion_tokens") or raw.get("output_tokens") or 0),
                         "total_tokens": int(raw.get("total_tokens") or 0)}
                continue
            if event in _TEXT_EVENTS:
                text = _message_text(data)
                if text:
                    if text.startswith(snapshot) and len(text) >= len(snapshot):
                        delta = text[len(snapshot):]  # 累积快照：计算增量
                        snapshot = text
                    else:
                        # 非前缀帧（上游改发增量片段）：必须输出，否则丢字。
                        delta = text
                        snapshot = snapshot + text
                else:
                    delta = ""
                if delta:
                    yield _openai_chunk(chunk_id, model, {"content": delta})
            elif event == "done":
                finish = _finish_reason(data if isinstance(data, dict) else {})
                saw_done = True
                break
        if not saw_done:
            # 上游 EOF 缺 done：按协议视为不完整回合，明确报错而非伪装成功。
            error_body = {"error": {"message": "trae event stream ended without done", "type": "incomplete_stream",
                                    "code": "incomplete_stream"}}
            yield f"data: {json.dumps(error_body, ensure_ascii=False)}\n\n".encode("utf-8")
            yield b"data: [DONE]\n\n"
            return
        yield _openai_chunk(chunk_id, model, {}, finish=finish, usage=usage)
        yield b"data: [DONE]\n\n"
    except Exception as exc:  # 断流/解析失败也要以 SSE 错误帧收尾，不留悬挂流
        error_body = {"error": {"message": f"trae stream interrupted: {type(exc).__name__}",
                                "type": "stream_error", "code": "stream_error"}}
        yield f"data: {json.dumps(error_body, ensure_ascii=False)}\n\n".encode("utf-8")
        yield b"data: [DONE]\n\n"


async def events_to_single(source, model: str) -> bytes:
    """非流式：消费事件流（有界，由调用方超时约束）并合成 OpenAI JSON 响应。"""
    text_parts: list[str] = []
    usage = None
    finish = "stop"
    saw_done = False
    async for name, data in source:
        event = normalize_event(name)
        if event in _TEXT_EVENTS:
            text = _message_text(data if isinstance(data, dict) else {})
            if text:
                if text.startswith("".join(text_parts)) and len(text) >= len("".join(text_parts)):
                    text_parts = [text]
                else:
                    text_parts.append(text)
        elif event == "token_usage":
            raw = data.get("usage") if isinstance(data, dict) else None
            raw = raw if isinstance(raw, dict) else (data if isinstance(data, dict) else {})
            usage = {"prompt_tokens": int(raw.get("prompt_tokens") or raw.get("input_tokens") or 0),
                     "completion_tokens": int(raw.get("completion_tokens") or raw.get("output_tokens") or 0),
                     "total_tokens": int(raw.get("total_tokens") or 0)}
        elif event == "done":
            finish = _finish_reason(data if isinstance(data, dict) else {})
            saw_done = True
            break
        elif event == "error":
            payload = data if isinstance(data, dict) else {}
            raise ValueError(f"trae upstream error: {payload.get('message') or payload.get('error') or 'unknown'}")
    if not saw_done:
        raise ValueError("trae event stream ended without done")
    body = {"id": "chatcmpl-" + uuid.uuid4().hex, "object": "chat.completion", "created": 0, "model": model,
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "".join(text_parts)},
                         "finish_reason": finish}],
            "usage": usage or {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}}
    return json.dumps(body, ensure_ascii=False).encode("utf-8")
