from __future__ import annotations

import time
from datetime import datetime


class TaskGuard:
    def __init__(self, config, runtimes):
        self.config = config
        self.runtimes = runtimes
        self.kill_switch = True

    def preview(self, gid, task_type, account_id=None):
        runtime = self.runtimes[gid]
        cfg = runtime.config
        accounts = [a["id"] for a in runtime.pool.accounts if a["enabled"] and (account_id is None or a["id"] == account_id)]
        capability = runtime.adapter.capabilities().get(task_type, {"status": "evidence_required"})
        current = datetime.now().strftime("%H:%M")
        start, end = cfg.task_window_start, cfg.task_window_end
        within = start <= current <= end if start <= end else current >= start or current <= end
        reasons = []
        if self.kill_switch:
            reasons.append("global_kill_switch")
        if not cfg.tasks_enabled:
            reasons.append("gateway_tasks_disabled")
        if not within:
            reasons.append("outside_execution_window")
        if capability["status"] != "supported":
            reasons.append("evidence_required")
        if not accounts:
            reasons.append("no_eligible_accounts")
        return {"dry_run": True, "allowed": not reasons, "gateway_id": gid, "task_type": task_type,
                "accounts": accounts, "daily_limit": cfg.task_daily_limit, "window_timezone": "server local time",
                "reason": reasons, "missing_evidence": capability.get("missing_evidence", [])}

    async def record_preview(self, gid, task_type, result):
        import json
        db = self.runtimes[gid].repository.db
        await db.write("INSERT INTO task_runs(gateway_id,task_type,idempotency_key,status,started_at,finished_at,result_json) VALUES(?,?,?,?,?,?,?)",
                       (gid, task_type, f"preview:{time.time_ns()}", "dry_run", time.time(), time.time(), json.dumps(result)))
