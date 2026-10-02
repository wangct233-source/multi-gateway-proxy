from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_version(version INTEGER PRIMARY KEY, applied_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS gateways(id TEXT PRIMARY KEY, name TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1);
CREATE TABLE IF NOT EXISTS accounts(
 id TEXT NOT NULL, gateway_id TEXT NOT NULL REFERENCES gateways(id), provider_account_id TEXT NOT NULL,
 secret_ref TEXT NOT NULL, secret_inline TEXT NOT NULL DEFAULT '', enabled INTEGER NOT NULL DEFAULT 1,
 status TEXT NOT NULL DEFAULT 'ready', cooldown_until REAL NOT NULL DEFAULT 0, metadata_json TEXT NOT NULL DEFAULT '{}',
 PRIMARY KEY(gateway_id,id), UNIQUE(gateway_id,provider_account_id));
CREATE TABLE IF NOT EXISTS egress(
 id TEXT NOT NULL, gateway_id TEXT NOT NULL REFERENCES gateways(id), role TEXT NOT NULL CHECK(role IN ('primary','backup')),
 endpoint_ref TEXT NOT NULL, healthy INTEGER NOT NULL DEFAULT 0, checked_at REAL,
 PRIMARY KEY(gateway_id,id), UNIQUE(gateway_id,role));
CREATE TABLE IF NOT EXISTS leases(
 lease_id TEXT PRIMARY KEY, account_id TEXT NOT NULL, gateway_id TEXT NOT NULL,
 expires_at REAL NOT NULL, state TEXT NOT NULL, released_at REAL,
 FOREIGN KEY(gateway_id,account_id) REFERENCES accounts(gateway_id,id));
CREATE INDEX IF NOT EXISTS leases_expiry ON leases(gateway_id,state,expires_at);
CREATE TABLE IF NOT EXISTS task_runs(
 id INTEGER PRIMARY KEY, gateway_id TEXT NOT NULL REFERENCES gateways(id), account_id TEXT,
 task_type TEXT NOT NULL, idempotency_key TEXT NOT NULL, status TEXT NOT NULL,
 started_at REAL NOT NULL, finished_at REAL, result_json TEXT NOT NULL DEFAULT '{}',
 UNIQUE(gateway_id,task_type,idempotency_key));
CREATE TABLE IF NOT EXISTS request_logs(
 id INTEGER PRIMARY KEY, gateway_id TEXT NOT NULL REFERENCES gateways(id), request_id TEXT NOT NULL,
 account_id TEXT, egress_id TEXT, status_code INTEGER, duration_ms REAL,
 streamed INTEGER NOT NULL, error_class TEXT, created_at REAL NOT NULL);
CREATE INDEX IF NOT EXISTS request_log_date ON request_logs(created_at);
CREATE TABLE IF NOT EXISTS settings(
 gateway_id TEXT NOT NULL REFERENCES gateways(id), key TEXT NOT NULL, value_json TEXT NOT NULL,
 PRIMARY KEY(gateway_id,key));
"""

# v2: accounts 增加 secret_inline（导入的真实凭据）。仅加列，老代码 SELECT */INSERT 均兼容，
# 因此 user_version 保持 1 不动，用 schema_version 表记录迁移进度，保证更新回滚后旧代码仍能启动。
MIGRATIONS = {2: ("ALTER TABLE accounts ADD COLUMN secret_inline TEXT NOT NULL DEFAULT ''",)}


class Database:
    def __init__(self, path: Path, queue_size: int = 4096):
        self.path = path
        self.queue = asyncio.Queue(queue_size)
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="sqlite")
        self.connection = None
        self.writer = None
        self.dropped_logs = 0
        self.last_error = None

    async def start(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        await self._thread(self._open)
        self.writer = asyncio.create_task(self._write_loop())

    def _open(self):
        self.connection = sqlite3.connect(self.path, timeout=5)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self.connection.execute("PRAGMA foreign_keys=ON")
        version = self.connection.execute("PRAGMA user_version").fetchone()[0]
        if version > 1:
            raise RuntimeError("Database schema newer than application; downgrade refused")
        self.connection.executescript(SCHEMA)
        self.connection.execute("INSERT OR IGNORE INTO schema_version VALUES(1,?)", (time.time(),))
        # 逐版本应用幂等迁移；列存在时 ALTER 报错则视为已迁移。
        applied = {row[0] for row in self.connection.execute("SELECT version FROM schema_version")}
        for number, statements in sorted(MIGRATIONS.items()):
            if number in applied:
                continue
            for statement in statements:
                try:
                    self.connection.execute(statement)
                except sqlite3.OperationalError as exc:
                    if "duplicate column" not in str(exc).lower():
                        raise
            self.connection.execute("INSERT INTO schema_version VALUES(?,?)", (number, time.time()))
        self.connection.execute("PRAGMA user_version=1")
        self.connection.execute("UPDATE leases SET state='expired' WHERE state='active'")
        self.connection.commit()

    async def _thread(self, function, *args):
        return await asyncio.get_running_loop().run_in_executor(self.executor, function, *args)

    def _batch(self, items):
        results = []
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            for sql, params, _ in items:
                self.connection.execute("SAVEPOINT item")
                try:
                    cursor = self.connection.execute(sql, params)
                    results.append(cursor.lastrowid)
                    self.connection.execute("RELEASE item")
                except sqlite3.Error as exc:
                    self.connection.execute("ROLLBACK TO item")
                    self.connection.execute("RELEASE item")
                    results.append(exc)
            self.connection.commit()
            return results
        except BaseException:
            self.connection.rollback()
            raise

    async def _write_loop(self):
        while True:
            item = await self.queue.get()
            if item is None:
                self.queue.task_done()
                break
            batch = [item]
            while len(batch) < 64 and not self.queue.empty():
                candidate = self.queue.get_nowait()
                if candidate is None:
                    self.queue.task_done()
                    await self.queue.put(None)
                    break
                batch.append(candidate)
            try:
                results = await self._thread(self._batch, batch)
                for (_, _, future), result in zip(batch, results):
                    if isinstance(result, Exception):
                        if future and not future.done():
                            future.set_exception(result)
                        elif future is None:
                            self.last_error = type(result).__name__
                    elif future and not future.done():
                        future.set_result(result)
            except Exception as exc:
                self.last_error = type(exc).__name__
                for _, _, future in batch:
                    if future and not future.done():
                        future.set_exception(exc)
            finally:
                for _ in batch:
                    self.queue.task_done()

    async def write(self, sql, params=()):
        future = asyncio.get_running_loop().create_future()
        await self.queue.put((sql, params, future))
        return await future

    async def enqueue_required(self, sql, params=()):
        await self.queue.put((sql, params, None))

    def enqueue(self, sql, params=()):
        try:
            self.queue.put_nowait((sql, params, None))
        except asyncio.QueueFull:
            self.dropped_logs += 1

    async def rows(self, sql, params=()):
        def read():
            return [dict(row) for row in self.connection.execute(sql, params).fetchall()]
        return await self._thread(read)

    async def close(self):
        await self.queue.join()
        await self.queue.put(None)
        await self.writer
        await self._thread(self.connection.close)
        self.executor.shutdown(wait=True)


class Repository:
    def __init__(self, database: Database):
        self.db = database
        self.account_lock = asyncio.Lock()

    async def seed(self, configs):
        for gid, cfg in configs.items():
            await self.db.write("INSERT OR IGNORE INTO gateways(id,name) VALUES(?,?)", (gid, cfg.name))
            for role in ("primary", "backup"):
                await self.db.write("INSERT OR IGNORE INTO egress(id,gateway_id,role,endpoint_ref) VALUES(?,?,?,?)",
                                    (role, gid, role, f"env:{cfg.prefix}_EGRESS_{role.upper()}"))
            for account in cfg.accounts:
                await self.add_account(gid, account, seed=True)

    async def add_account(self, gid, item, seed=False):
        async with self.account_lock:
            return await self._add_account(gid, item, seed)

    async def _add_account(self, gid, item, seed=False):
        from .config import secret_value
        identifier = str(item.get("id", ""))
        provider = str(item.get("provider_account_id", ""))
        ref = str(item.get("secret_ref", ""))
        inline = str(item.get("secret_inline") or "")
        if not identifier or len(identifier) > 128 or len(provider) > 256:
            raise ValueError("id and provider_account_id are required")
        if inline:
            # 导入路径：真实凭据直接入库（与原项目 accounts/*.json 同级安全性）。
            if len(inline) > 16384 or not inline.strip():
                raise ValueError("invalid inline credential")
            ref = ""
        else:
            secret_value(ref)
            ref = "env:" + ref.removeprefix("env:")
        others = await self.db.rows("SELECT gateway_id,secret_ref,secret_inline FROM accounts WHERE gateway_id<>?", (gid,))
        token = inline or (secret_value(ref) if ref else "")
        def resolved(row):
            if row["secret_inline"]:
                return row["secret_inline"]
            try:
                return secret_value(row["secret_ref"])
            except ValueError:
                return ""
        if token and any(resolved(row) == token for row in others):
            raise ValueError("Account credential cannot be reused across gateways")
        metadata = item.get("metadata", {})
        def contains_secret(value):
            if isinstance(value, dict):
                return any(str(k).lower() in {"token", "password", "authorization", "refresh_token", "access_token", "cookie", "secret", "api_key", "apikey"}
                           or contains_secret(v) for k, v in value.items())
            if isinstance(value, list):
                return any(contains_secret(v) for v in value)
            return False
        if not isinstance(metadata, dict) or contains_secret(metadata):
            raise ValueError("metadata cannot contain credentials")
        statement = "INSERT OR IGNORE" if seed else "INSERT"
        await self.db.write(f"{statement} INTO accounts(id,gateway_id,provider_account_id,secret_ref,secret_inline,enabled,metadata_json) VALUES(?,?,?,?,?,?,?)",
                            (identifier, gid, provider, ref, inline, int(bool(item.get("enabled", True))), json.dumps(metadata)))

    async def accounts(self, gid):
        rows = await self.db.rows("SELECT * FROM accounts WHERE gateway_id=?", (gid,))
        for row in rows:
            row["enabled"] = bool(row["enabled"])
            row["metadata"] = json.loads(row.pop("metadata_json"))
        return rows

    async def settings(self, gid):
        return {row["key"]: json.loads(row["value_json"]) for row in await self.db.rows("SELECT * FROM settings WHERE gateway_id=?", (gid,))}

    async def set_settings(self, gid, values):
        for key, value in values.items():
            await self.db.write("INSERT INTO settings VALUES(?,?,?) ON CONFLICT(gateway_id,key) DO UPDATE SET value_json=excluded.value_json",
                                (gid, key, json.dumps(value)))
