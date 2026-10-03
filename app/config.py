from __future__ import annotations

import json
import math
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

GATEWAYS = {"a-cn": ("A_CN", "A-1 腾讯国内"), "a-intl": ("A_INTL", "A-2 腾讯国际"),
            "b": ("B", "B TRAE CN"), "c": ("C", "C Zcode")}


def boolean(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).lower() in {"true", "1", "yes"}


def number(name: str, default: float, minimum: float = 0) -> float:
    value = float(os.getenv(name, str(default)))
    if not math.isfinite(value) or value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return value


def secret_value(ref: str) -> str:
    name = ref.removeprefix("env:")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError("secret_ref must be env:VARIABLE_NAME")
    if name in {"ADMIN_TOKEN", "DATA_TOKENS", "UPDATES_WEBHOOK_SECRET", "GH_TOKEN", "GITHUB_TOKEN"} or name.startswith("GH_"):
        raise ValueError("Internal credential cannot be used as an upstream account secret")
    value = os.getenv(name, "")
    internal = {os.getenv("ADMIN_TOKEN", ""), os.getenv("UPDATES_WEBHOOK_SECRET", ""),
                *os.getenv("DATA_TOKENS", "").split(",")}
    if value and value in internal:
        raise ValueError("Upstream credential equals an internal credential")
    return value


def validate_url(value: str, proxy: bool = False) -> None:
    if proxy and value == "direct://local":
        return
    if not value:
        return
    parsed = urlsplit(value)
    schemes = {"http", "https", "socks5"} if proxy else {"http", "https"}
    if parsed.scheme not in schemes or not parsed.hostname:
        raise ValueError("Unsupported or invalid URL (value redacted)")
    if not proxy and (parsed.username or parsed.password or parsed.query or parsed.fragment):
        raise ValueError("Upstream URL cannot contain credentials, query or fragment")


@dataclass
class GatewayConfig:
    id: str
    name: str
    prefix: str
    upstream_url: str = ""
    mode: str = "disabled"
    primary: str = ""
    backup: str = ""
    check_url: str = "https://example.com"
    concurrency: int = 8
    account_concurrency: int = 2
    queue_limit: int = 32
    queue_timeout: float = 15
    connections: int = 16
    keepalive: int = 8
    tasks_enabled: bool = False
    task_window_start: str = "00:00"
    task_window_end: str = "23:59"
    task_daily_limit: int = 1
    accounts: list[dict] = field(default_factory=list)
    models: list[str] = field(default_factory=list)


@dataclass
class Config:
    admin_token: str
    data_tokens: tuple[str, ...]
    database: Path
    gateways: dict[str, GatewayConfig]
    cors_origins: list[str]
    tasks_kill_switch: bool = True
    body_limit: int = 1048576
    stream_total: float = 600
    stream_idle: float = 60
    connect_timeout: float = 10
    soft_memory_mb: float = 256
    memory_watermark: float = .8
    egress_check_seconds: float = 60
    updates_enabled: bool = False
    updates_poll_seconds: float = 300
    updates_secret: str = ""
    updates_repo: str = ""
    updates_branch: str = "main"
    updates_auto_apply: bool = False
    repo_dir: Path = field(default_factory=lambda: Path(__file__).resolve().parents[1])

    @classmethod
    def from_env(cls) -> Config:
        token = os.getenv("ADMIN_TOKEN", "")
        if len(token) < 24:
            raise ValueError("ADMIN_TOKEN is required and must contain at least 24 characters")
        gateways = {}
        for gid, (prefix, name) in GATEWAYS.items():
            get = lambda suffix, default="": os.getenv(f"{prefix}_{suffix}", default)
            item = GatewayConfig(
                gid, name, prefix, get("UPSTREAM_URL"), get("UPSTREAM_MODE", "disabled"),
                get("EGRESS_PRIMARY"), get("EGRESS_BACKUP"), get("EGRESS_CHECK_URL", "https://example.com"),
                int(number(f"{prefix}_CONCURRENCY", 8, 1)),
                int(number(f"{prefix}_ACCOUNT_CONCURRENCY", 2, 1)),
                int(number(f"{prefix}_QUEUE_LIMIT", 32)),
                number(f"{prefix}_QUEUE_TIMEOUT", 15, .001),
                int(number(f"{prefix}_HTTP_MAX_CONNECTIONS", 16, 1)),
                int(number(f"{prefix}_HTTP_KEEPALIVE", 8)),
                boolean(f"{prefix}_TASKS_ENABLED"), get("TASK_WINDOW_START", "00:00"),
                get("TASK_WINDOW_END", "23:59"), int(number(f"{prefix}_TASK_DAILY_LIMIT", 1, 1)),
                json.loads(get("ACCOUNTS_JSON", "[]")),
                [m.strip() for m in get("MODELS").split(",") if m.strip()],
            )
            if item.mode not in {"disabled", "openai", "a", "b-remote", "c-anthropic"}:
                raise ValueError(f"Invalid {prefix}_UPSTREAM_MODE")
            if item.connections < item.concurrency or item.keepalive > item.connections:
                raise ValueError(f"{prefix} connection pool must cover concurrency")
            for window in (item.task_window_start, item.task_window_end):
                if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", window):
                    raise ValueError(f"Invalid {prefix} task window")
            validate_url(item.upstream_url)
            validate_url(item.check_url)
            validate_url(item.primary, True)
            validate_url(item.backup, True)
            gateways[gid] = item
        seen = set()
        for item in gateways.values():
            for url in (item.primary, item.backup):
                if not url or url == "direct://local":
                    continue
                parsed = urlsplit(url)
                endpoint = (parsed.hostname.lower(), parsed.port or 8080)
                if endpoint in seen:
                    raise ValueError("Proxy endpoint cannot be shared across gateway roles")
                seen.add(endpoint)
        origins = [x.strip() for x in os.getenv("CORS_ORIGINS", "").split(",") if x.strip()]
        if "*" in origins:
            raise ValueError("CORS wildcard is not allowed")
        if boolean("ALLOW_NULL_ORIGIN"):
            origins.append("null")
        data = tuple(x.strip() for x in os.getenv("DATA_TOKENS", "").split(",") if x.strip()) or (token,)
        watermark = number("MEMORY_WATERMARK", .8, .1)
        if watermark > 1:
            raise ValueError("MEMORY_WATERMARK must be <= 1")
        return cls(token, data, Path(os.getenv("DATABASE_PATH", "data/proxy.db")), gateways, origins,
                   boolean("TASKS_KILL_SWITCH", True), int(number("REQUEST_BODY_LIMIT", 1048576, 1024)),
                   number("STREAM_TOTAL_TIMEOUT", 600, 1), number("STREAM_IDLE_TIMEOUT", 60, 1),
                   number("CONNECT_TIMEOUT", 10, .1), number("SOFT_MEMORY_MB", 256, 1), watermark,
                   number("EGRESS_CHECK_SECONDS", 60, 1), boolean("UPDATES_ENABLED"),
                   number("UPDATES_POLL_SECONDS", 300, 10), os.getenv("UPDATES_WEBHOOK_SECRET", ""),
                   os.getenv("UPDATES_REPO_SLUG", ""), os.getenv("UPDATES_BRANCH", "main"),
                   boolean("UPDATES_AUTO_APPLY"))
