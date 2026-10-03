#!/usr/bin/env python3
"""Local OpenAI-shaped fixture only; no provider access or identity emulation."""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

MODES = {"normal", "tool", "slow", "idle", "error", "close", "sse-error"}
BODY_LIMIT = 1048576


class MockServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "LocalMock/1"
    sys_version = ""

    def log_message(self, fmt, *args):
        # Do not log URLs, bodies or Authorization, even for error requests.
        pass

    def send_json(self, status, body):
        data = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
        self.wfile.flush()

    def do_GET(self):
        path = urlsplit(self.path).path.rstrip("/")
        if path == "/healthz" or path.endswith("/healthz"):
            self.send_json(200, {"status": "ok", "fixture": "local-only"})
        elif path.endswith("/v1/models"):
            self.send_json(200, {"object": "list", "data": [
                {"id": "mock-" + mode, "object": "model", "owned_by": "local-fixture"}
                for mode in sorted(MODES)
            ]})
        # b-remote 两步协议第二步：事件流
        elif "/chat_sessions/" in path and path.endswith("/events"):
            self.send_trae_events()
        else:
            self.send_json(404, {"error": {"message": "fixture route not found"}})

    def send_trae_events(self):
        """私有事件帧：累积快照 message + heartbeat + token_usage + done。"""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()

        def frame(event, data):
            self.wfile.write(f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n".encode("utf-8"))
            self.wfile.flush()

        self.wfile.write(b": heartbeat\n\n")
        self.wfile.flush()
        frame("message", {"message": {"content": "你好"}})
        time.sleep(self.server.options.delay)
        self.wfile.write(b": heartbeat\n\n")
        self.wfile.flush()
        frame("message", {"message": {"content": "你好，世界。"}})
        frame("token_usage", {"usage": {"prompt_tokens": 8, "completion_tokens": 6, "total_tokens": 14}})
        frame("done", {"reason": "stop"})
        self.wfile.write(b"data: [DONE]\n\n")

    def do_POST(self):
        parsed = urlsplit(self.path)
        route = parsed.path.rstrip("/")
        # b-remote 两步协议第一步：创建会话（必须消费请求体，keep-alive 才不会错位）
        if route.endswith("/chat_sessions"):
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if size > 0:
                    self.connection.settimeout(10)
                    self.rfile.read(size)
            except (ValueError, TimeoutError):
                self.close_connection = True
            self.send_json(200, {"code": 0, "data": {"chat_session_id": "mock-session-1", "message_id": "mock-msg-1"}})
            return
        # c-anthropic：Anthropic messages 协议
        if route.endswith("/v1/messages") or route.endswith("/messages"):
            self.handle_anthropic()
            return
        # OpenAI 兼容路径（原有）
        if not route.endswith("/chat/completions"):
            self.close_connection = True
            self.send_json(404, {"error": {"message": "fixture route not found"}})
            return
        try:
            if self.headers.get("Transfer-Encoding"):
                raise ValueError("fixture expects Content-Length")
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= BODY_LIMIT:
                self.close_connection = True
                self.send_json(413, {"error": {"message": "fixture body limit exceeded"}})
                return
            self.connection.settimeout(10)
            body = json.loads(self.rfile.read(size))
            if not isinstance(body, dict):
                raise ValueError("JSON object required")
            model = body.get("model", "mock-normal")
            if not isinstance(model, str):
                raise ValueError("model must be a string")
            mode = parse_qs(parsed.query).get("mode", [model.removeprefix("mock-")])[0]
            if mode not in MODES:
                mode = "normal"  # arbitrary normal model names are accepted
        except (ValueError, UnicodeError, TimeoutError):
            self.close_connection = True
            self.send_json(400, {"error": {"message": "invalid fixture request"}})
            return
        if mode == "error":
            self.send_json(503, {"error": {"message": "intentional local mock error", "type": "mock_error"}})
            return
        try:
            if body.get("stream", False):
                self.stream_reply(body, model, mode)
            else:
                self.nonstream_reply(body, model, mode)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, TimeoutError):
            self.close_connection = True  # expected when testing cancellation

    def handle_anthropic(self):
        """Anthropic messages 形态：流式事件 / 非流式 JSON（含 tool_use）。"""
        try:
            if self.headers.get("Transfer-Encoding"):
                raise ValueError("fixture expects Content-Length")
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= BODY_LIMIT:
                raise ValueError("body limit")
            self.connection.settimeout(10)
            body = json.loads(self.rfile.read(size))
            if not isinstance(body, dict):
                raise ValueError("JSON object required")
        except (ValueError, UnicodeError, TimeoutError):
            self.close_connection = True
            self.send_json(400, {"type": "error", "error": {"type": "invalid_request_error", "message": "bad fixture request"}})
            return
        model = str(body.get("model") or "mock-anthropic")
        stream = bool(body.get("stream"))
        tools = isinstance(body.get("tools"), list) and bool(body["tools"])
        want_tool = tools or any(
            isinstance(m, dict) and m.get("role") == "user" and isinstance(m.get("content"), list)
            and any(isinstance(b, dict) and b.get("type") == "tool_result" for b in m["content"])
            for m in body.get("messages", []))
        if stream:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()

            def frame(event, data):
                self.wfile.write(f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n".encode("utf-8"))
                self.wfile.flush()

            frame("message_start", {"type": "message_start", "message": {"id": "msg_mock", "role": "assistant"}})
            frame("content_block_start", {"type": "content_block_start", "index": 0,
                                          "content_block": {"type": "text", "text": ""}})
            frame("content_block_delta", {"type": "content_block_delta", "index": 0,
                                          "delta": {"type": "text_delta", "text": "你好"}})
            time.sleep(self.server.options.delay)
            frame("content_block_delta", {"type": "content_block_delta", "index": 0,
                                          "delta": {"type": "text_delta", "text": "，世界。"}})
            frame("content_block_stop", {"type": "content_block_stop", "index": 0})
            if want_tool:
                frame("content_block_start", {"type": "content_block_start", "index": 1,
                                              "content_block": {"type": "tool_use", "id": "toolu_mock", "name": "mock_lookup"}})
                frame("content_block_delta", {"type": "content_block_delta", "index": 1,
                                              "delta": {"type": "input_json_delta", "partial_json": '{"城市":'}})
                frame("content_block_delta", {"type": "content_block_delta", "index": 1,
                                              "delta": {"type": "input_json_delta", "partial_json": '"北京"}'}})
                frame("content_block_stop", {"type": "content_block_stop", "index": 1})
            stop = "tool_use" if want_tool else "end_turn"
            frame("message_delta", {"type": "message_delta", "delta": {"stop_reason": stop},
                                    "usage": {"output_tokens": 6}})
            frame("message_stop", {"type": "message_stop"})
            return
        content = [{"type": "text", "text": "你好，世界。"}]
        if want_tool:
            content.append({"type": "tool_use", "id": "toolu_mock", "name": "mock_lookup",
                            "input": {"城市": "北京"}})
        self.send_json(200, {"id": "msg_mock", "type": "message", "role": "assistant", "model": model,
                             "content": content, "stop_reason": "tool_use" if want_tool else "end_turn",
                             "usage": {"input_tokens": 8, "output_tokens": 6}})

    @staticmethod
    def tool_name(body):
        tools = body.get("tools", [])
        if isinstance(tools, list) and tools and isinstance(tools[0], dict):
            function = tools[0].get("function", {})
            if isinstance(function, dict) and isinstance(function.get("name"), str):
                return function["name"]
        return "mock_lookup"

    def nonstream_reply(self, body, model, mode):
        if mode in {"slow", "idle"}:
            time.sleep(self.server.options.slow_delay if mode == "slow" else self.server.options.idle_seconds)
        if mode == "close":
            self.close_connection = True
            self.connection.shutdown(2)
            return
        if mode == "sse-error":
            self.send_json(200, {"error": {"message": "intentional application-level error"}})
            return
        tool = mode == "tool" or bool(body.get("tools"))
        message = {"role": "assistant", "content": "你好，世界。"}
        if tool:
            message["tool_calls"] = [{"id": "call_local", "type": "function", "function": {
                "name": self.tool_name(body), "arguments": '{"城市":"北京"}'}}]
        self.send_json(200, {"id": "chatcmpl-mock-" + uuid.uuid4().hex, "object": "chat.completion",
                             "created": int(time.time()), "model": model,
                             "choices": [{"index": 0, "message": message,
                                          "finish_reason": "tool_calls" if tool else "stop"}],
                             "usage": {"prompt_tokens": 8, "completion_tokens": 6, "total_tokens": 14}})

    def stream_reply(self, body, model, mode):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Accel-Buffering", "no")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        cid = "chatcmpl-mock-" + uuid.uuid4().hex
        created = int(time.time())

        def raw(data):
            self.wfile.write(f"{len(data):X}\r\n".encode("ascii") + data + b"\r\n")
            self.wfile.flush()

        def frame(delta, finish=None, usage=None):
            value = {"id": cid, "object": "chat.completion.chunk", "created": created, "model": model,
                     "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
            if usage is not None:
                value["usage"] = usage
            data = ("data: " + json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n\n").encode("utf-8")
            # Deliberately split UTF-8 and frames across transport chunks. Clients must
            # parse the SSE byte stream, not assume one HTTP chunk equals one event.
            cut = data.find("你".encode("utf-8"))
            cut = cut + 1 if cut >= 0 else max(1, len(data) // 2)
            raw(data[:cut])
            raw(data[cut:])

        raw(b": local mock heartbeat\n\n")
        frame({"role": "assistant", "content": ""})
        if mode == "idle":
            time.sleep(self.server.options.idle_seconds)  # no heartbeat during idle gap
        elif mode == "slow":
            time.sleep(self.server.options.slow_delay)
        else:
            time.sleep(self.server.options.delay)
        if mode == "sse-error":
            raw(b'data: {"error":{"message":"intentional SSE error"}}\n\n')
        else:
            frame({"content": "你好"})
            time.sleep(self.server.options.delay)
            raw(b": local mock heartbeat\n\n")
            if mode != "close":
                frame({"content": "，世界。"})
                tool = mode == "tool" or bool(body.get("tools"))
                if tool:
                    frame({"tool_calls": [{"index": 0, "id": "call_local", "type": "function",
                                           "function": {"name": self.tool_name(body), "arguments": ""}}]})
                    frame({"tool_calls": [{"index": 0, "function": {"arguments": '{"城市":'}}]})
                    frame({"tool_calls": [{"index": 0, "function": {"arguments": '"北京"}'}}]})
                frame({}, "tool_calls" if tool else "stop",
                      {"prompt_tokens": 8, "completion_tokens": 6, "total_tokens": 14})
        if mode != "close":
            raw(b"data: [DONE]\n\n")
        # close intentionally has a valid HTTP ending but no SSE terminator.
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1", help="default loopback; use 0.0.0.0 only for a protected Docker-host test")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--delay", type=float, default=0.03, help="normal inter-frame delay, seconds")
    parser.add_argument("--slow-delay", type=float, default=1.0, help="slow-mode first-content delay, seconds")
    parser.add_argument("--idle-seconds", type=float, default=90.0)
    options = parser.parse_args()
    if not 1 <= options.port <= 65535 or any(not math.isfinite(x) or x < 0 for x in
                                           (options.delay, options.slow_delay, options.idle_seconds)):
        parser.error("invalid port or delay")
    server = MockServer((options.host, options.port), Handler)
    server.options = options
    print(f"local mock listening on {options.host}:{options.port}; not a public/provider verification", file=sys.stderr)
    try:
        server.serve_forever(poll_interval=0.1)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
