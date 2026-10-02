#!/usr/bin/env python3
"""Bounded-memory HTTP/SSE benchmark; JSON metrics only, never token/body output."""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import re
import statistics
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from urllib.parse import urlsplit

GATEWAYS = ("a-cn", "a-intl", "b", "c")
MAX_FRAME_BYTES = 1048576


class StreamFailure(Exception):
    pass


@dataclass
class Observation:
    latency_ms: float = 0
    status: int | None = None
    success: bool = False
    failure: str | None = None
    first_byte_ms: float | None = None
    first_data_event_ms: float | None = None
    ttft_ms: float | None = None
    raw_bytes: int = 0
    data_events: int = 0
    comment_lines: int = 0
    content_events: int = 0
    done: bool = False


class SSEParser:
    """Incrementally frame SSE bytes; neither buffer whole responses nor count tokens."""
    def __init__(self, observation, started):
        self.observation = observation
        self.started = started
        self.pending = bytearray()
        self.data = []
        self.frame_size = 0
        self.event_type = ""
        self.first_line = True

    def feed(self, chunk):
        # HTTP chunks may contain many events; cap only the incomplete line/frame.
        self.pending.extend(chunk)
        consumed = 0
        while consumed < len(self.pending):
            lf = self.pending.find(b"\n", consumed)
            cr = self.pending.find(b"\r", consumed)
            ends = [i for i in (lf, cr) if i >= 0]
            if not ends:
                break
            end = min(ends)
            if self.pending[end] == 13 and end + 1 == len(self.pending):
                break  # CRLF may straddle HTTP chunks
            skip = 2 if self.pending[end:end + 2] == b"\r\n" else 1
            self.line(bytes(self.pending[consumed:end]))
            consumed = end + skip
        if consumed:
            del self.pending[:consumed]
        if len(self.pending) + self.frame_size > MAX_FRAME_BYTES:
            raise StreamFailure("sse_frame_too_large")

    def line(self, line):
        if self.first_line:
            line = line.removeprefix(b"\xef\xbb\xbf")
            self.first_line = False
        if not line:
            if self.data:
                self.dispatch(b"\n".join(self.data))
            elif self.event_type == "error":
                raise StreamFailure("sse_error")
            self.data.clear()
            self.frame_size = 0
            self.event_type = ""
            return
        self.frame_size += len(line) + 1
        if self.frame_size > MAX_FRAME_BYTES:
            raise StreamFailure("sse_frame_too_large")
        if line.startswith(b":"):
            self.observation.comment_lines += 1
            return
        key, _, value = line.partition(b":")
        if value.startswith(b" "):
            value = value[1:]
        if key == b"data":
            self.data.append(value)
        elif key == b"event":
            self.event_type = value.decode("utf-8", "strict")

    def dispatch(self, data):
        obs = self.observation
        obs.data_events += 1
        elapsed = (time.perf_counter() - self.started) * 1000
        if obs.first_data_event_ms is None:
            obs.first_data_event_ms = elapsed
        if self.event_type == "error":
            raise StreamFailure("sse_error")
        if data.strip() == b"[DONE]":
            obs.done = True
            return
        try:
            body = json.loads(data)
        except (ValueError, UnicodeError):
            raise StreamFailure("invalid_sse_json") from None
        if not isinstance(body, dict):
            raise StreamFailure("invalid_sse_object")
        if body.get("error"):
            raise StreamFailure("sse_error")
        choices = body.get("choices", [])
        if not isinstance(choices, list):
            raise StreamFailure("invalid_sse_choices")
        meaningful = False
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            delta = choice.get("delta", {})
            if not isinstance(delta, dict):
                continue
            meaningful |= any(isinstance(delta.get(k), str) and bool(delta[k])
                              for k in ("content", "reasoning_content", "refusal"))
            calls = delta.get("tool_calls", [])
            if isinstance(calls, list):
                for call in calls:
                    function = call.get("function", {}) if isinstance(call, dict) else {}
                    if isinstance(function, dict):
                        meaningful |= bool(function.get("name") or function.get("arguments"))
        if meaningful:
            if obs.done:
                raise StreamFailure("content_after_done")
            obs.content_events += 1
            if obs.ttft_ms is None:
                obs.ttft_ms = elapsed

    def finish(self):
        # A final CR is a valid SSE line ending even without a following LF.
        if self.pending.endswith(b"\r"):
            self.line(bytes(self.pending[:-1]))
            self.pending.clear()
        if self.pending or self.data or self.event_type:
            raise StreamFailure("truncated_sse_frame")
        if not self.observation.done:
            raise StreamFailure("missing_done")
        if not self.observation.content_events:
            raise StreamFailure("empty_stream")


def percentile(values, percent):
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * percent / 100
    low, high = math.floor(index), math.ceil(index)
    return round(ordered[low] + (ordered[high] - ordered[low]) * (index - low), 3)


def distribution(values):
    return {"samples": len(values), "p50": percentile(values, 50), "p95": percentile(values, 95),
            "mean": round(statistics.fmean(values), 3) if values else None}


@dataclass
class Group:
    results: list[Observation] = field(default_factory=list)
    inflight: int = 0
    peak: int = 0

    def enter(self):
        self.inflight += 1
        self.peak = max(self.peak, self.inflight)

    def leave(self):
        self.inflight -= 1

    def report(self, elapsed):
        good = [r for r in self.results if r.success]
        return {
            "attempted_requests": len(self.results), "successful_requests": len(good),
            "failed_requests": len(self.results) - len(good),
            "failure_rate": (len(self.results) - len(good)) / len(self.results) if self.results else 0,
            "elapsed_seconds": round(elapsed, 6),
            "request_qps": round(len(self.results) / elapsed, 6),
            "successful_request_qps": round(len(good) / elapsed, 6),
            "client_inflight_peak": self.peak,
            "total_latency_ms_all": distribution([r.latency_ms for r in self.results]),
            "total_latency_ms_success": distribution([r.latency_ms for r in good]),
            "ttft_ms_success": distribution([r.ttft_ms for r in good if r.ttft_ms is not None]),
            "first_body_byte_ms_all": distribution([r.first_byte_ms for r in self.results if r.first_byte_ms is not None]),
            "first_data_event_ms_all": distribution([r.first_data_event_ms for r in self.results if r.first_data_event_ms is not None]),
            "http_status_counts": dict(Counter(str(r.status) for r in self.results if r.status is not None)),
            "failure_counts": dict(Counter(r.failure for r in self.results if not r.success)),
            "sse_raw_bytes": sum(r.raw_bytes for r in self.results),
            "sse_data_events": sum(r.data_events for r in self.results),
            "sse_comment_lines": sum(r.comment_lines for r in self.results),
            "sse_content_events": sum(r.content_events for r in self.results),
            "streams_with_done": sum(r.done for r in self.results),
        }


async def one_request(client, url, payload, args, httpx):
    obs = Observation()
    started = time.perf_counter()
    try:
        # Includes pool/connect/read time and slow streams even if heartbeats keep
        # resetting the per-read idle timeout. No automatic retry hides failures.
        async with asyncio.timeout(args.total_timeout):
            async with client.stream("POST", url, json=payload) as response:
                obs.status = response.status_code
                if not 200 <= response.status_code < 300:
                    raise StreamFailure("http_status")
                if args.stream:
                    if response.headers.get("content-type", "").split(";")[0].strip().lower() != "text/event-stream":
                        raise StreamFailure("not_sse")
                    parser = SSEParser(obs, started)
                    async for chunk in response.aiter_raw():
                        if not chunk:
                            continue
                        if obs.first_byte_ms is None:
                            obs.first_byte_ms = (time.perf_counter() - started) * 1000
                        obs.raw_bytes += len(chunk)
                        parser.feed(chunk)
                    parser.finish()
                else:
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        if chunk and obs.first_byte_ms is None:
                            obs.first_byte_ms = (time.perf_counter() - started) * 1000
                        body.extend(chunk)
                        if len(body) > MAX_FRAME_BYTES:
                            raise StreamFailure("json_body_too_large")
                    value = json.loads(body)
                    if not isinstance(value, dict) or value.get("error") or not value.get("choices"):
                        raise StreamFailure("invalid_completion")
                obs.success = True
    except StreamFailure as exc:
        obs.failure = str(exc)  # fixed internal category only; no response text
    except httpx.ReadTimeout:
        obs.failure = "idle_timeout"
    except httpx.ConnectTimeout:
        obs.failure = "connect_timeout"
    except httpx.PoolTimeout:
        obs.failure = "client_pool_timeout"
    except TimeoutError:
        obs.failure = "total_timeout"
    except httpx.HTTPError as exc:
        obs.failure = "transport_error:" + type(exc).__name__
    except (ValueError, UnicodeError):
        obs.failure = "invalid_response"
    finally:
        obs.latency_ms = (time.perf_counter() - started) * 1000
    return obs


async def benchmark(args, token, httpx):
    selected = GATEWAYS if args.gateway == "all" else (args.gateway,)
    groups = {gateway: Group() for gateway in selected}
    overall = Group()
    gate = asyncio.Event()
    slots = args.clients * len(selected)
    limits = httpx.Limits(max_connections=slots, max_keepalive_connections=slots)
    timeout = httpx.Timeout(connect=args.connect_timeout, read=args.idle_timeout,
                            write=args.idle_timeout, pool=args.connect_timeout)
    payload = {"model": args.model, "messages": [{"role": "user", "content": "你好，请简短回答。"}],
               "stream": args.stream, "max_tokens": 32}
    if args.tools:
        payload["tools"] = [{"type": "function", "function": {"name": "mock_lookup", "parameters": {
            "type": "object", "properties": {"城市": {"type": "string"}}}}}]
    async with httpx.AsyncClient(headers={"Authorization": "Bearer " + token,
                                          "Accept-Encoding": "identity"},
                                 limits=limits, timeout=timeout, follow_redirects=False, trust_env=False) as client:
        async def worker(gateway, count):
            await gate.wait()
            group = groups[gateway]
            url = args.base_url.rstrip("/") + args.path_template.format(gateway=gateway)
            for _ in range(count):
                group.enter()
                overall.enter()
                try:
                    result = await one_request(client, url, payload, args, httpx)
                    group.results.append(result)
                    overall.results.append(result)
                finally:
                    group.leave()
                    overall.leave()

        tasks = [asyncio.create_task(worker(gateway, args.requests // args.clients + (index < args.requests % args.clients)))
                 for gateway in selected for index in range(min(args.clients, args.requests))]
        started = time.perf_counter()
        gate.set()  # all four gateways share the same start barrier
        await asyncio.gather(*tasks)
        elapsed = max(time.perf_counter() - started, 1e-9)
    report = {
        "schema_version": 1,
        "configuration": {"gateways": list(selected), "clients_per_gateway": args.clients,
                          "requests_per_gateway": args.requests, "stream": args.stream,
                          "connect_timeout_seconds": args.connect_timeout,
                          "idle_timeout_seconds": args.idle_timeout, "total_timeout_seconds": args.total_timeout},
        "metric_notes": {
            "request_qps": "completed attempts divided by shared wall-clock duration; not tokens/s or SSE events/s",
            "client_inflight_peak": "simultaneous client requests, not server/account concurrency; clients is per gateway",
            "ttft": "first nonempty content/reasoning/refusal/tool delta; excludes role, heartbeat and usage-only events",
            "latency": "client dispatch through response EOF/close; includes admission/queue/network time; no automatic retries",
            "stream_success": "2xx SSE, valid JSON data frames, meaningful delta, [DONE], clean EOF, no error event",
            "scope": "target chosen by caller; this output alone never proves provider or public-network verification",
        },
        "overall": overall.report(elapsed),
        "gateways": {gateway: group.report(elapsed) for gateway, group in groups.items()},
    }
    print(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False))
    return 0 if all(r.success for r in overall.results) else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--token-env", default="ADMIN_TOKEN", help="read the token only from this environment variable")
    parser.add_argument("--clients", type=int, default=4, help="concurrent clients PER gateway")
    parser.add_argument("--requests", type=int, default=20, help="total requests PER gateway")
    parser.add_argument("--stream", action="store_true")
    parser.add_argument("--gateway", choices=(*GATEWAYS, "all"), default="all")
    parser.add_argument("--model", default="mock-normal")
    parser.add_argument("--tools", action="store_true", help="include a fixture tool schema")
    parser.add_argument("--path-template", default="/gw/{gateway}/v1/chat/completions",
                        help="override only for a deliberate direct mock/control test")
    parser.add_argument("--connect-timeout", type=float, default=10)
    parser.add_argument("--idle-timeout", type=float, default=60)
    parser.add_argument("--total-timeout", type=float, default=600)
    args = parser.parse_args()
    try:
        parsed = urlsplit(args.base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError
        parsed.port  # validate without revealing input
    except ValueError:
        parser.error("base URL must be HTTP(S), without credentials/query/fragment")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.token_env):
        parser.error("invalid token environment variable name")
    token = os.environ.get(args.token_env, "")
    if not token or any(ord(c) < 32 or ord(c) > 126 for c in token):
        parser.error("token environment variable is missing or invalid (value redacted)")
    if args.clients < 1 or args.requests < 1 or any(not math.isfinite(x) or x <= 0 for x in
                                                (args.connect_timeout, args.idle_timeout, args.total_timeout)):
        parser.error("clients, requests and timeouts must be positive")
    try:
        formatted = args.path_template.format(gateway="a-cn")
        if not formatted.startswith("/") or formatted.startswith("//") or urlsplit(formatted).fragment:
            raise ValueError
    except (KeyError, IndexError, ValueError):
        parser.error("invalid path template; supported placeholder is {gateway}")
    try:
        import httpx
    except ImportError:
        parser.error("httpx is missing; use the existing application venv with its pinned requirements")
    try:
        return asyncio.run(benchmark(args, token, httpx))
    except KeyboardInterrupt:
        print(json.dumps({"error": "interrupted; incomplete run excluded"}), file=sys.stderr)
        return 130
    except Exception:
        # Do not serialize exception strings: libraries can embed URLs/headers.
        print(json.dumps({"error": "benchmark setup/runtime failure; no credentials emitted"}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
