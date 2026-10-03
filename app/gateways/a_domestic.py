from __future__ import annotations

import base64
import hashlib
import json
import time

from .base import GatewayAdapter, upstream_url

# 官方桌面客户端的 realm 常量（来源：wb_identity.py / wb_accounts.py REALM_CONFIGS）。
# 这些是协议必需的出站身份参数，不携带任何账号凭据。
REALMS = {
    "cn": {
        "origin": "https://www.codebuddy.cn",
        "domain": "copilot.tencent.com",
        "user_agent": "WorkBuddy/5.5.6 WorkBuddy/5.5.6 CLI/2.137.1",
        "ide_version": "5.5.6",
        "language": "zh-CN",
    },
    "intl": {
        "origin": "https://www.workbuddy.ai",
        "domain": "www.workbuddy.ai",
        "user_agent": "WorkBuddy/5.5.2 WorkBuddy AI/5.5.2 CLI/5.5.2",
        "ide_version": "5.5.2",
        "language": "en-US",
    },
}


def _derive_id(uid: str, salt: str) -> str:
    """machine/session 标识：同账号稳定派生，避免随机漂移（wb_fingerprint.py:11-17）。"""
    seed = f"{salt}:{uid or 'anonymous'}"
    return hashlib.md5(seed.encode("utf-8")).hexdigest()


def _request_id(uid: str) -> str:
    return _derive_id(uid, "req") + "-" + str(time.time_ns() % 1_000_000).zfill(6)


def _jwt_sub(token: str) -> str:
    """从 access token 的 JWT sub claim 提取 uid（wb_accounts.py:161-176 同款解析）。"""
    try:
        segment = token.split(".")[1]
        segment += "=" * (-len(segment) % 4)
        return str(json.loads(base64.urlsafe_b64decode(segment)).get("sub") or "")
    except Exception:
        return ""


class DomesticAdapter(GatewayAdapter):
    realm = "cn"
    source_evidence = "wb_identity.py:76-105; wb_accounts.py:119-144,449-504; wb_fingerprint.py:11-24"

    def identity_headers(self, lease):
        cfg = REALMS[self.realm]
        # JWT sub 是权威 uid 来源；provider_account_id 仅作无法解析时的兜底。
        uid = _jwt_sub(lease.token) or str(lease.account.get("provider_account_id") or "") or "anonymous"
        return {
            "Authorization": "Bearer " + lease.token,
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "identity",
            "X-Agent-Purpose": "conversation",
            "X-IDE-Type": "WorkBuddy",
            "X-IDE-Name": "WorkBuddy",
            "X-IDE-Version": cfg["ide_version"],
            "X-Product": "WorkBuddy",
            "X-Domain": cfg["domain"],
            "X-User-Id": uid,
            "X-Request-ID": _request_id(uid),
            "X-Machine-ID": _derive_id(uid, "machine"),
            "X-Session-ID": _derive_id(uid, "session"),
            "X-CodeBuddy-Request": "1",
            "User-Agent": cfg["user_agent"],
            "Origin": cfg["origin"],
            "Referer": cfg["origin"] + "/",
            "Accept-Language": cfg["language"],
        }

    def prepare(self, path, payload, lease):
        path, data = self._validated_payload(path, payload)
        return upstream_url(self.config.upstream_url, path), self.identity_headers(lease), data


# 刷新协议（wb_accounts.py:525-565 同构）：轮换式 refreshToken，必须串行。
REFRESH_PATH = "v2/plugin/auth/token/refresh"
# 余额/套餐查询（fetch_credits，wb_accounts.py:150-151,766-815 同构）。
CREDITS_PATH = "v2/billing/meter/get-user-resource"
# 计费域与 chat 域可不同（analysis_A_domestic：国内 billing=www.codebuddy.cn）。
BILLING_BASE = {"cn": "https://www.codebuddy.cn", "intl": "https://www.workbuddy.ai"}
_REFRESH_LOCKS: dict[str, "asyncio.Lock"] = {}


def credits_request(realm: str, lease) -> tuple[str, dict, dict]:
    """构造余额查询请求（billing 身份头，purpose=billing 同款）。返回 (url, headers, body)。"""
    cfg = REALMS[realm]
    uid = _jwt_sub(lease.token) or str(lease.account.get("provider_account_id") or "") or "anonymous"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": cfg["user_agent"],
        "Origin": cfg["origin"],
        "Referer": cfg["origin"] + "/",
        "Authorization": "Bearer " + lease.token,
        "X-User-Id": uid,
        "X-Domain": cfg["domain"],
        "X-CodeBuddy-Request": "1",
        "X-Machine-ID": _derive_id(uid, "machine"),
        "X-Session-ID": _derive_id(uid, "session"),
        "Accept-Language": cfg["language"],
    }
    if realm == "cn":
        headers["X-Product"] = "SaaS"
    body = {"PageNumber": 1, "PageSize": 20, "ProductCode": "p_tcaca",
            "Status": [0, 3], "PackageEndTime": ""}
    return upstream_url(BILLING_BASE[realm], CREDITS_PATH), headers, body


def parse_credits(payload) -> dict:
    """聚合套餐余量（data.Response.Data.Accounts[] 按套餐累加 remain/used/size）。"""
    accounts = []
    try:
        accounts = (payload or {}).get("data", {}).get("Response", {}).get("Data", {}).get("Accounts") or []
    except AttributeError:
        return {}
    remain = used = size = 0
    packages = 0
    for pkg in accounts:
        if not isinstance(pkg, dict):
            continue
        for cycle in (pkg.get("Cycles") or pkg.get("Cycle") or [pkg]):
            if not isinstance(cycle, dict):
                continue
            try:
                remain += float(cycle.get("Remain") or cycle.get("remain") or 0)
                used += float(cycle.get("Used") or cycle.get("used") or 0)
                size += float(cycle.get("Size") or cycle.get("size") or 0)
            except (TypeError, ValueError):
                continue
        packages += 1
    if packages == 0:
        return {}
    return {"remain": remain, "used": used, "size": size, "packages": packages}


def refresh_lock(account_key: str) -> "asyncio.Lock":
    import asyncio
    lock = _REFRESH_LOCKS.get(account_key)
    if lock is None:
        lock = asyncio.Lock()
        _REFRESH_LOCKS[account_key] = lock
    return lock


def refresh_request(base_url: str, realm: str, refresh_token: str, uid: str) -> tuple[str, dict, dict]:
    """构造 token 刷新请求：(url, headers, body)。响应 data 含轮换后的 accessToken/refreshToken。"""
    cfg = REALMS[realm]
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": cfg["user_agent"],
        "Origin": cfg["origin"],
        "Referer": cfg["origin"] + "/",
        "X-Refresh-Token": refresh_token,
        "X-Auth-Refresh-Source": "workbuddy" if realm == "cn" else "plugin",
        "X-User-Id": uid or "anonymous",
        "X-Domain": cfg["domain"],
        "X-CodeBuddy-Request": "1",
        "Accept-Language": cfg["language"],
    }
    return upstream_url(base_url, REFRESH_PATH), headers, {}
