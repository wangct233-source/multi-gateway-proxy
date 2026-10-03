"""管理页登录鉴权：密码哈希存 admin_auth 表（单行 id=1），默认 admin。

- 存储格式：每安装随机 salt + sha256，磁盘不留明文；每次请求用内存缓存快速比对。
- env ADMIN_TOKEN 仍是应急主钥匙（忘记密码时救援用），两者任一通过即可。
- 公网暴露 + 默认弱密码的爆破防护：同一 IP 连续错 5 次锁 60 秒（内存态，重启即清）。
"""
from __future__ import annotations

import hashlib
import hmac
import secrets
import time

DEFAULT_PASSWORD = "admin"
FAIL_LIMIT = 5
LOCK_SECONDS = 60


def _fast_hash(salt: str, password: str) -> str:
    return hashlib.sha256((salt + password).encode("utf-8")).hexdigest()


class AdminAuth:
    def __init__(self):
        self.salt = ""
        self.hash = ""
        self.is_default = True
        self._fails: dict[str, list] = {}

    async def load(self, repository):
        """启动时装载；首次启动播种默认密码 admin。

        is_default 不落库，每次从哈希反推：存的就是 admin 的哈希即视为默认密码，
        避免进程重启后丢失该标志（云端实测踩坑）。
        """
        rows = await repository.db.rows("SELECT password_salt,password_hash FROM admin_auth WHERE id=1")
        if rows:
            self.salt = str(rows[0]["password_salt"])
            self.hash = str(rows[0]["password_hash"])
            self.is_default = self.verify(DEFAULT_PASSWORD)
            return
        self.set_password(DEFAULT_PASSWORD)
        await self.save(repository)

    async def save(self, repository):
        await repository.db.write(
            "INSERT INTO admin_auth(id,password_salt,password_hash,updated_at) VALUES(1,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET password_salt=excluded.password_salt,"
            "password_hash=excluded.password_hash,updated_at=excluded.updated_at",
            (self.salt, self.hash, time.time()))

    def set_password(self, password: str):
        self.salt = secrets.token_hex(16)
        self.hash = _fast_hash(self.salt, password)
        self.is_default = password == DEFAULT_PASSWORD

    def verify(self, password) -> bool:
        supplied = str(password or "")
        if not supplied or not self.hash:
            return False
        return hmac.compare_digest(_fast_hash(self.salt, supplied), self.hash)

    def locked(self, ip: str) -> bool:
        entry = self._fails.get(ip)
        return bool(entry and entry[1] > time.time())

    def note_failure(self, ip: str):
        entry = self._fails.setdefault(ip, [0, 0.0])
        entry[0] += 1
        if entry[0] >= FAIL_LIMIT:
            entry[0] = 0
            entry[1] = time.time() + LOCK_SECONDS
        if len(self._fails) > 4096:
            # 公网扫描器会制造大量一次性 IP；只清理未锁定的旧条目防无界增长。
            now = time.time()
            for key in [k for k, v in self._fails.items() if v[1] <= now][:1024]:
                self._fails.pop(key, None)

    def note_success(self, ip: str):
        self._fails.pop(ip, None)
