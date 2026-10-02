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
