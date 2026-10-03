"""Zcode Anthropic 协议转换（G4 / c-anthropic）。

机制来源（只读参考，未复制代码）：ZcodeKnight src/proxy/upstream.ts:58-128,
src/translator/（openai↔anthropic 双向与 SSE 翻译）。
协议要点：
- 上游为 Anthropic Messages API：POST {base}/v1/messages。
- coding-plan 双认证头 ``x-api-key`` + ``Authorization: Bearer``（凭据为
  Z.AI 的 ``apiKey.secret`` 拼接或 Bigmodel 纯 apiKey，导入时已拼好）。
- 签名路径 fail-open，LLM 主路径免签（client-signing.ts:42-44, 81-85）。
- SSE 事件：message_start / content_block_delta(text_delta|input_json_delta) /
  content_block_stop / message_delta(stop_reason,usage) / message_stop。
"""
from __future__ import annotations

import json
import uuid

ANTHROPIC_VERSION = "2023-06-01"
SDK_UA = "ZCode/3.12.3 ai-sdk/anthropic/3.0.81"

_STOP_MAP = {"end_turn": "stop", "stop_sequence": "stop", "max_tokens": "length",
             "tool_use": "tool_calls", "refusal": "content_filter"}


def anthropic_headers(credential: str, stream: bool) -> dict:
    return {
        "x-api-key": credential,
        "Authorization": "Bearer " + credential,
        "anthropic-version": ANTHROPIC_VERSION,
        "Content-Type": "application/json",
        "Accept": "text/event-stream" if stream else "application/json",
        "User-Agent": SDK_UA,
        "Accept-Encoding": "identity",
    }


# ── 余额查询（强证据：routes-quota.ts:219-258 — billing/balance 需 Bearer JWT）──
ZCODE_PLAN_ORIGIN = "https://zcode.z.ai"
BALANCE_PATH = "api/v1/zcode-plan/billing/balance"


def balance_request(jwt: str) -> tuple[str, dict]:
    """构造套餐余额查询。仅 start-plan JWT 凭据可用；apiKey 无此查询证据。"""
    url = f"{ZCODE_PLAN_ORIGIN}/{BALANCE_PATH}?app_version=3.12.3&platform=windows"
    return url, {"Authorization": "Bearer " + jwt, "Accept": "application/json",
                 "User-Agent": SDK_UA, "Accept-Encoding": "identity"}


def parse_c_credits(payload) -> dict:
    """聚合 data.balances[]（remaining_units/total_units/used_units）。"""
    try:
        balances = (payload or {}).get("data", {}).get("balances") or []
    except AttributeError:
        return {}
    remain = used = total = 0.0
    items = []
    for entry in balances:
        if not isinstance(entry, dict):
            continue
        try:
            r = float(entry.get("remaining_units") or 0)
            u = float(entry.get("used_units") or 0)
            t = float(entry.get("total_units") or 0)
        except (TypeError, ValueError):
            continue
        remain += r
        used += u
        total += t
        items.append({"name": str(entry.get("show_name") or ""), "remaining": r,
                      "unit_type": str(entry.get("unit_type") or ""),
                      "expires_at": entry.get("expires_at")})
    if not items:
        return {}
    return {"remaining": remain, "used": used, "total": total, "packages": items}


def _content_text(content) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and str(block.get("type") or "") in {"text", "output_text"}:
                text = block.get("text") or block.get("content")
                if isinstance(text, str):
                    parts.append(text)
        return "".join(parts)
    return str(content)


def openai_to_anthropic(payload: dict) -> dict:
    """OpenAI chat 请求体 → Anthropic messages 请求体。"""
    if not isinstance(payload, dict) or not isinstance(payload.get("messages"), list):
        raise ValueError("messages must be an array")
    system_parts: list[str] = []
    messages: list[dict] = []
    tool_name_by_id: dict[str, str] = {}
    for m in payload["messages"]:
        if not isinstance(m, dict):
            continue
        role = str(m.get("role") or "user").lower()
        if role == "system":
            text = _content_text(m.get("content"))
            if text:
                system_parts.append(text)
            continue
        if role == "tool":
            result_content = _content_text(m.get("content"))
            messages.append({"role": "user", "content": [{
                "type": "tool_result", "tool_use_id": str(m.get("tool_call_id") or ""),
                "content": [{"type": "text", "text": result_content}]}]})
            continue
        blocks: list[dict] = []
        text = _content_text(m.get("content"))
        if text:
            blocks.append({"type": "text", "text": text})
        calls = m.get("tool_calls")
        if isinstance(calls, list):
            for call in calls:
                if not isinstance(call, dict):
                    continue
                fn = call.get("function") or {}
                name = str(fn.get("name") or "")
                call_id = str(call.get("id") or "")
                tool_name_by_id[call_id] = name
                arguments = fn.get("arguments")
                if isinstance(arguments, str):
                    try:
                        parsed = json.loads(arguments) if arguments.strip() else {}
                    except ValueError:
                        parsed = {"_raw": arguments}
                elif isinstance(arguments, dict):
                    parsed = arguments
                else:
                    parsed = {}
                blocks.append({"type": "tool_use", "id": call_id or ("toolu_" + uuid.uuid4().hex[:16]),
                               "name": name, "input": parsed})
        if blocks:
            messages.append({"role": "assistant" if role == "assistant" else "user", "content": blocks})
    body: dict = {"model": str(payload.get("model") or ""), "messages": messages, "max_tokens": 8192}
    if system_parts:
        body["system"] = "\n\n".join(system_parts)
    if isinstance(payload.get("max_tokens"), int) and payload["max_tokens"] > 0:
        body["max_tokens"] = payload["max_tokens"]
    for key in ("temperature", "top_p"):
        value = payload.get(key)
        if isinstance(value, (int, float)):
            body[key] = value
    if payload.get("stream"):
        body["stream"] = True
    tools = payload.get("tools")
    if isinstance(tools, list) and tools:
        converted = []
        for tool in tools:
            fn = (tool.get("function") if isinstance(tool, dict) else None) or {}
            if not fn.get("name"):
                continue
            converted.append({"name": str(fn["name"]),
                              "description": str(fn.get("description") or ""),
                              "input_schema": fn.get("parameters") if isinstance(fn.get("parameters"), dict)
                              else {"type": "object", "properties": {}}})
        if converted:
            body["tools"] = converted
    return body


def anthropic_to_openai(body: dict, model: str) -> dict:
    """非流式 Anthropic /v1/messages 响应 → OpenAI chat.completion JSON。"""
    blocks = body.get("content") if isinstance(body.get("content"), list) else []
    text_parts: list[str] = []
    tool_calls: list[dict] = []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        btype = str(block.get("type") or "")
        if btype == "text":
            text_parts.append(str(block.get("text") or ""))
        elif btype == "tool_use":
            tool_calls.append({"id": str(block.get("id") or ""), "type": "function",
                               "function": {"name": str(block.get("name") or ""),
                                            "arguments": json.dumps(block.get("input") or {}, ensure_ascii=False)}})
    message: dict = {"role": "assistant", "content": "".join(text_parts)}
    if tool_calls:
        message["tool_calls"] = tool_calls
    usage_raw = body.get("usage") if isinstance(body.get("usage"), dict) else {}
    prompt_t = int(usage_raw.get("input_tokens") or 0)
    completion_t = int(usage_raw.get("output_tokens") or 0)
    return {"id": "chatcmpl-" + uuid.uuid4().hex, "object": "chat.completion", "created": 0, "model": model,
            "choices": [{"index": 0, "message": message,
                         "finish_reason": _STOP_MAP.get(str(body.get("stop_reason") or ""), "stop")}],
            "usage": {"prompt_tokens": prompt_t, "completion_tokens": completion_t,
                      "total_tokens": prompt_t + completion_t}}


def _chunk(chunk_id: str, model: str, delta: dict, finish=None, usage=None) -> bytes:
    body = {"id": chunk_id, "object": "chat.completion.chunk", "created": 0, "model": model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
    if usage is not None:
        body["usage"] = usage
    return ("data: " + json.dumps(body, ensure_ascii=False, separators=(",", ":")) + "\n\n").encode("utf-8")


def _sse_frame(data: dict) -> bytes:
    return f"event: {data.get('type', 'message')}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n".encode("utf-8")


async def anthropic_sse_to_openai(byte_stream, model: str):
    """Anthropic SSE 字节流 → OpenAI chunk SSE 字节流。逐事件转换，不整包缓冲。"""
    chunk_id = "chatcmpl-" + uuid.uuid4().hex
    event_name = ""
    tool_index: dict[int, dict] = {}
    stop_reason = "stop"
    usage: dict | None = None
    opened = False
    buffer = b""
    try:
        yield _chunk(chunk_id, model, {"role": "assistant"})
        opened = True
        async for raw in byte_stream:
            buffer += raw
            while b"\n" in buffer:
                line_bytes, buffer = buffer.split(b"\n", 1)
                line = line_bytes.decode("utf-8", errors="replace").rstrip("\r")
                if not line or line.startswith(":"):
                    continue
                if line.startswith("event:"):
                    event_name = line[6:].strip()
                    continue
                if not line.startswith("data:"):
                    continue
                payload_text = line[5:].strip()
                if not payload_text or payload_text == "[DONE]":
                    continue
                try:
                    data = json.loads(payload_text)
                except ValueError:
                    continue
                if not isinstance(data, dict):
                    continue
                etype = str(data.get("type") or event_name or "")
                event_name = ""
                if etype == "content_block_delta":
                    delta = data.get("delta") or {}
                    dtype = str(delta.get("type") or "")
                    if dtype == "text_delta":
                        text = delta.get("text")
                        if isinstance(text, str) and text:
                            yield _chunk(chunk_id, model, {"content": text})
                    elif dtype == "input_json_delta":
                        index = int(data.get("index") or 0)
                        partial = delta.get("partial_json") or ""
                        entry = tool_index.setdefault(index, {"args": "", "emitted": False})
                        entry["args"] += partial
                        if not entry["emitted"] and tool_index.get(index, {}).get("name"):
                            yield _chunk(chunk_id, model, {"tool_calls": [
                                {"index": index, "id": tool_index[index].get("id", ""),
                                 "type": "function", "function": {"name": tool_index[index].get("name", ""),
                                                                  "arguments": ""}}]})
                            entry["emitted"] = True
                        if partial:
                            yield _chunk(chunk_id, model, {"tool_calls": [
                                {"index": index, "function": {"arguments": partial}}]})
                elif etype == "content_block_start":
                    block = data.get("content_block") or {}
                    if str(block.get("type") or "") == "tool_use":
                        index = int(data.get("index") or 0)
                        tool_index[index] = {"id": str(block.get("id") or ""), "name": str(block.get("name") or ""),
                                             "args": "", "emitted": False}
                elif etype == "message_delta":
                    delta = data.get("delta") or {}
                    stop_reason = _STOP_MAP.get(str(delta.get("stop_reason") or ""), stop_reason)
                    raw_usage = data.get("usage")
                    if isinstance(raw_usage, dict):
                        usage = {"prompt_tokens": int(raw_usage.get("input_tokens") or 0),
                                 "completion_tokens": int(raw_usage.get("output_tokens") or 0)}
                        usage["total_tokens"] = usage["prompt_tokens"] + usage["completion_tokens"]
                elif etype == "message_stop":
                    break
                elif etype == "error":
                    error_body = {"error": {"message": str(data.get("error", {}).get("message") or "upstream error"),
                                            "type": "upstream_error", "code": "upstream_error"}}
                    yield f"data: {json.dumps(error_body, ensure_ascii=False)}\n\n".encode("utf-8")
                    yield b"data: [DONE]\n\n"
                    return
        yield _chunk(chunk_id, model, {}, finish=stop_reason, usage=usage)
        yield b"data: [DONE]\n\n"
    except Exception as exc:
        if not opened:
            raise
        error_body = {"error": {"message": f"anthropic stream interrupted: {type(exc).__name__}",
                                "type": "stream_error", "code": "stream_error"}}
        yield f"data: {json.dumps(error_body, ensure_ascii=False)}\n\n".encode("utf-8")
        yield b"data: [DONE]\n\n"
