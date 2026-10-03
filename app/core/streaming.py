from __future__ import annotations

import asyncio
import time

from starlette.responses import Response

class GeneratorResponse(Response):
    """把任意异步字节迭代器以原样流式响应发出（协议转换层的载体）。

    与 UpstreamResponse 相同的断连检测 / 总期限 / 清理契约；迭代器由
    协议适配器提供，禁止整包缓冲。
    """

    def __init__(self, chunks, cleanup, total_timeout, idle_timeout, status_code=200,
                 content_type="text/event-stream"):
        self.chunks = chunks
        self.cleanup = cleanup
        self.total_timeout = total_timeout
        self.idle_timeout = idle_timeout
        self.status_code = status_code
        self.raw_headers = [(b"content-type", content_type.encode()),
                            (b"cache-control", b"no-cache, no-transform"),
                            (b"x-accel-buffering", b"no")]
        self.done_reason = "completed"

    async def __call__(self, scope, receive, send):
        async def body():
            await send({"type": "http.response.start", "status": self.status_code, "headers": self.raw_headers})
            deadline = time.monotonic() + self.total_timeout
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("stream_total_timeout")
                try:
                    chunk = await asyncio.wait_for(anext(self.chunks), min(remaining, self.idle_timeout))
                except StopAsyncIteration:
                    break
                if chunk:
                    await send({"type": "http.response.body", "body": chunk, "more_body": True})
            await send({"type": "http.response.body", "body": b"", "more_body": False})

        async def disconnected():
            while True:
                event = await receive()
                if event["type"] == "http.disconnect":
                    return

        sender = asyncio.create_task(body())
        receiver = asyncio.create_task(disconnected())
        try:
            done, _ = await asyncio.wait({sender, receiver}, return_when=asyncio.FIRST_COMPLETED)
            if sender in done:
                try:
                    await sender
                except TimeoutError:
                    self.done_reason = "upstream_timeout"
                    raise
                except Exception:
                    self.done_reason = "upstream_or_client_error"
                    raise
            else:
                self.done_reason = "client_disconnect"
        except asyncio.CancelledError:
            self.done_reason = "cancelled"
            raise
        finally:
            sender.cancel()
            receiver.cancel()
            await asyncio.gather(sender, receiver, return_exceptions=True)
            task = asyncio.create_task(self.cleanup(self.done_reason))
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                await task
                raise


HOP_HEADERS = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te",
               "trailer", "transfer-encoding", "upgrade", "set-cookie"}


class UpstreamResponse(Response):
    def __init__(self, upstream, cleanup, total_timeout, idle_timeout):
        self.upstream = upstream
        self.cleanup = cleanup
        self.total_timeout = total_timeout
        self.idle_timeout = idle_timeout
        self.status_code = upstream.status_code
        nominated = {h.strip().lower() for h in upstream.headers.get("connection", "").split(",")}
        self.raw_headers = [(k, v) for k, v in upstream.headers.raw
                            if k.decode("latin1").lower() not in HOP_HEADERS | nominated]
        if "text/event-stream" in upstream.headers.get("content-type", ""):
            self.raw_headers += [(b"cache-control", b"no-cache, no-transform"), (b"x-accel-buffering", b"no")]
        self.background = None
        self.done_reason = "completed"

    async def __call__(self, scope, receive, send):
        async def body():
            await send({"type": "http.response.start", "status": self.status_code, "headers": self.raw_headers})
            iterator = self.upstream.aiter_raw()
            deadline = time.monotonic() + self.total_timeout
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("stream_total_timeout")
                try:
                    chunk = await asyncio.wait_for(anext(iterator), min(remaining, self.idle_timeout))
                except StopAsyncIteration:
                    break
                if chunk:
                    await send({"type": "http.response.body", "body": chunk, "more_body": True})
            await send({"type": "http.response.body", "body": b"", "more_body": False})

        async def disconnected():
            while True:
                event = await receive()
                if event["type"] == "http.disconnect":
                    return

        async def bounded_body():
            async with asyncio.timeout(self.total_timeout):
                await body()

        sender = asyncio.create_task(bounded_body())
        receiver = asyncio.create_task(disconnected())
        try:
            done, _ = await asyncio.wait({sender, receiver}, return_when=asyncio.FIRST_COMPLETED)
            if sender in done:
                try:
                    await sender
                except TimeoutError:
                    self.done_reason = "upstream_timeout"
                    # A timed-out partial frame must not be followed by a fabricated success/DONE frame.
                    raise
                except Exception:
                    self.done_reason = "upstream_or_client_error"
                    raise
            else:
                self.done_reason = "client_disconnect"
        except asyncio.CancelledError:
            self.done_reason = "cancelled"
            raise
        finally:
            sender.cancel()
            receiver.cancel()
            await asyncio.gather(sender, receiver, return_exceptions=True)
            task = asyncio.create_task(self.cleanup(self.done_reason))
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                await task
                raise
