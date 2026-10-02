from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass

from app.config import secret_value


@dataclass
class Lease:
    id: str
    gateway_id: str
    account: dict
    token: str
    expires_at: float
    released: bool = False


class AccountPool:
    def __init__(self, gid, repository, limit=2, ttl=600):
        self.gid = gid
        self.repository = repository
        self.limit = limit
        self.ttl = ttl
        self.accounts = []
        self.in_flight = {}
        self.leases = {}
        self.affinity = {}
        self.cursor = 0
        self.lock = asyncio.Lock()

    async def load(self):
        accounts = await self.repository.accounts(self.gid)
        async with self.lock:
            self.accounts = accounts

    async def acquire(self, session_key=None, exclude=None, only_account=None):
        async with self.lock:
            now = time.time()
            def credential(account):
                # 导入账号的 inline 凭据优先；否则解析 env 引用（解析失败视为无凭据）。
                if account.get("secret_inline"):
                    return account["secret_inline"]
                try:
                    return secret_value(account["secret_ref"])
                except ValueError:
                    return ""
            eligible = [a for a in self.accounts if a["enabled"] and
                        a["status"] != "disabled" and a["cooldown_until"] <= now and
                        self.in_flight.get(a["id"], 0) < self.limit and
                        a["id"] not in (exclude or set()) and
                        (only_account is None or a["id"] == only_account) and credential(a)]
            if not eligible:
                return None
            selected = None
            if session_key:
                binding = self.affinity.get(session_key)
                if binding and binding[1] > now:
                    selected = next((a for a in eligible if a["id"] == binding[0]), None)
                    if selected is None:
                        bound = next((a for a in self.accounts if a["id"] == binding[0]), None)
                        if bound and bound["enabled"] and bound["cooldown_until"] <= now:
                            return None
            if selected is None:
                selected = eligible[self.cursor % len(eligible)]
                self.cursor += 1
            if session_key:
                self.affinity[session_key] = (selected["id"], now + 1800)
                if len(self.affinity) > 5000:
                    self.affinity = {k: v for k, v in self.affinity.items() if v[1] > now}
                    while len(self.affinity) > 5000:
                        self.affinity.pop(next(iter(self.affinity)))
            aid = selected["id"]
            lease = Lease(uuid.uuid4().hex, self.gid, dict(selected), credential(selected), now + self.ttl)
            await self.repository.db.enqueue_required("INSERT INTO leases(lease_id,account_id,gateway_id,expires_at,state) VALUES(?,?,?,?,?)",
                                                     (lease.id, aid, self.gid, lease.expires_at, "active"))
            self.in_flight[aid] = self.in_flight.get(aid, 0) + 1
            self.leases[lease.id] = lease
            return lease

    async def release(self, lease):
        async with self.lock:
            current = self.leases.get(lease.id)
            if current is None or current.released:
                return
            await self.repository.db.enqueue_required("UPDATE leases SET state='released',released_at=? WHERE lease_id=?",
                                                      (time.time(), lease.id))
            current.released = True
            aid = current.account["id"]
            self.in_flight[aid] = max(0, self.in_flight.get(aid, 0) - 1)
            self.leases.pop(lease.id, None)

    async def report(self, lease, status):
        if status not in {401, 403, 429}:
            return
        async with self.lock:
            account = next((a for a in self.accounts if a["id"] == lease.account["id"]), None)
            if account:
                cooldown = time.time() + (60 if status == 429 else 15)
                account["cooldown_until"] = cooldown
                self.affinity = {k: v for k, v in self.affinity.items() if v[0] != account["id"]}
                await self.repository.db.enqueue_required("UPDATE accounts SET cooldown_until=? WHERE gateway_id=? AND id=?",
                                                          (cooldown, self.gid, account["id"]))

    def public_accounts(self):
        # secret_inline 永不出现在任何 API 响应中。
        return [{"id": a["id"], "provider_account_id": a["provider_account_id"], "enabled": a["enabled"],
                 "status": a["status"],
                 "source": "imported" if a.get("secret_inline") else "env:" + a["secret_ref"].removeprefix("env:"),
                 "cooldown_until": a["cooldown_until"]} |
                {"in_flight": self.in_flight.get(a["id"], 0)} for a in self.accounts]
