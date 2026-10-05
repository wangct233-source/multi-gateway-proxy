from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

import httpx

log = logging.getLogger(__name__)


@dataclass
class Exit:
    role: str
    configured: bool
    client: httpx.AsyncClient | None = None
    healthy: bool = False
    error: str | None = None
    blocked_until: float = 0


class EgressPool:
    def __init__(self, config, global_config):
        self.config = config
        self.global_config = global_config
        self.exits = []
        self.lock = asyncio.Lock()
        self.last_error = None
        for role, url in (("primary", config.primary), ("backup", config.backup)):
            client = None
            if url:
                client = httpx.AsyncClient(
                    proxy=None if url == "direct://local" else url, trust_env=False, follow_redirects=False,
                    timeout=httpx.Timeout(global_config.stream_idle, connect=global_config.connect_timeout,
                                          pool=config.queue_timeout),
                    # 连接池给足静态上限：闸门并发=账号数×每账号并发（动态），上限只需"够大"。
                    limits=httpx.Limits(max_connections=1024, max_keepalive_connections=100),
                )
            self.exits.append(Exit(role, bool(url), client))

    async def check(self):
        async def probe(exit):
            if not exit.client:
                return
            if exit.blocked_until > time.monotonic():
                exit.healthy = False
                return
            try:
                response = await exit.client.get(self.config.check_url, timeout=5)
                exit.healthy = 200 <= response.status_code < 400
                exit.error = None if exit.healthy else "health_http_failure"
            except (httpx.HTTPError, OSError):
                exit.healthy, exit.error = False, "egress_health_unreachable"
        await asyncio.gather(*(probe(exit) for exit in self.exits))
        if not any(e.healthy for e in self.exits):
            if self.last_error != "no_healthy_egress":
                log.error("gateway=%s paused: no healthy egress (credential redacted)", self.config.id)
            self.last_error = "no_healthy_egress"
        else:
            self.last_error = None

    async def select(self):
        async with self.lock:
            if not self.exits[0].configured:
                self.last_error = "primary_egress_not_configured"
                return None
            return next((e for e in self.exits if e.healthy and e.blocked_until <= time.monotonic()), None)

    async def fail(self, exit, reason="transport_failure", duration: float = 60):
        async with self.lock:
            exit.healthy = False
            exit.error = reason
            exit.blocked_until = time.monotonic() + max(1, duration)
        log.warning("gateway=%s egress=%s blocked %.0fs: %s", self.config.id, exit.role, duration, reason)

    def public(self):
        return [{"id": e.role, "role": e.role, "healthy": e.healthy, "error": e.error,
                 "configured": e.configured} for e in self.exits]

    async def close(self):
        await asyncio.gather(*(e.client.aclose() for e in self.exits if e.client))
