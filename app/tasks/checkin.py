from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from datetime import datetime

from app.gateways.base import upstream_url

EVIDENCE = {
    "a-cn": ["analysis_A_domestic.md:581-607; wb_accounts.py:742-764"],
    "b": ["trae-反代/analysis_shtu:1560-1794; src/trae_client.py:557,570"],
}


class CheckinExecutor:
    def __init__(self, guard):
        self.guard = guard
        self.locks = {gid: asyncio.Lock() for gid in guard.runtimes}

    async def execute(self, gid, account_id=None):
        preview = self.guard.preview(gid, "checkin", account_id)
        if not preview["allowed"]:
            return {"executed": False, "status": "blocked", **preview}
        runtime = self.guard.runtimes[gid]
        prefix = runtime.config.prefix
        billing = os.getenv(f"{prefix}_BILLING_URL", "")
        if not billing:
            return {"executed": False, "status": "blocked", "reason": "billing_url_not_configured"}
        from app.config import validate_url
        validate_url(billing)
        if gid not in EVIDENCE:
            return {"executed": False, "status": "evidence_required"}
        results = []
        async with self.locks[gid]:
            # 批量任务的账号错峰：按本网关策略间隔 + 抖动（CodeBuddy 45s / Trae 60s）。
            from app.risk.base import jitter
            from app.risk.base import task_interval_for
            for index, aid in enumerate(preview["accounts"]):
                if index > 0:
                    await asyncio.sleep(jitter(task_interval_for(gid)))
                # Re-check the global switch between accounts, including after any awaited operation.
                if not self.guard.preview(gid, "checkin", aid)["allowed"]:
                    break
                today = datetime.now().date().isoformat()
                rows = await runtime.repository.db.rows(
                    "SELECT COUNT(*) AS n FROM task_runs WHERE gateway_id=? AND account_id=? AND task_type='checkin' AND status!='dry_run' AND started_at>=?",
                    (gid, aid, datetime.combine(datetime.now().date(), datetime.min.time()).timestamp()))
                if rows[0]["n"] >= runtime.config.task_daily_limit:
                    results.append({"account_id": aid, "status": "daily_limit"})
                    continue
                async with runtime.gate.slot():
                    exit = await runtime.egress.select()
                    if not exit:
                        results.append({"account_id": aid, "status": "no_healthy_egress"})
                        continue
                    lease = await runtime.pool.acquire(only_account=aid)
                    if not lease:
                        results.append({"account_id": aid, "status": "account_busy_or_unavailable"})
                        continue
                    run_id = None
                    result = {"account_id": aid, "status": "failed", "ok": False}
                    try:
                        metadata = lease.account["metadata"]
                        if gid == "b" and not metadata.get("device_id"):
                            result["status"] = "real_device_metadata_required"
                            continue
                        run_id = await runtime.repository.db.write(
                            "INSERT INTO task_runs(gateway_id,account_id,task_type,idempotency_key,status,started_at) VALUES(?,?,?,?,?,?)",
                            (gid, aid, "checkin", f"{today}:{aid}:{uuid.uuid4().hex}", "running", time.time()))
                        headers = {"Content-Type": "application/json", "Accept": "application/json"}
                        if gid == "a-cn":
                            headers["Authorization"] = "Bearer " + lease.token
                            headers["X-User-Id"] = lease.account["provider_account_id"]
                            path = "v2/billing/meter/daily-checkin"
                        else:
                            headers["Authorization"] = "Cloud-IDE-JWT " + lease.token
                            headers["x-device-id"] = metadata["device_id"]
                            path = "trae/api/v2/ug/checkin_credits/claim"
                        if not self.guard.preview(gid, "checkin", aid)["allowed"]:
                            result["status"] = "kill_switch_or_window_changed"
                            continue
                        response = await exit.client.post(upstream_url(billing, path), json={}, headers=headers, timeout=15)
                        body = response.json()
                        code = body.get("code")
                        result.update(ok=response.is_success and code == 0, business_code=code,
                                      status="accepted_pending_verification" if response.is_success and code == 0 else "rejected")
                    except Exception as exc:
                        result["error_class"] = type(exc).__name__
                    finally:
                        await runtime.pool.release(lease)
                        if run_id:
                            await runtime.repository.db.write("UPDATE task_runs SET status=?,finished_at=?,result_json=? WHERE id=?",
                                                              (result["status"], time.time(), json.dumps(result), run_id))
                        results.append(result)
        return {"executed": bool(results), "gateway_id": gid, "results": results,
                "note": "code=0 is acceptance only; actual reward is not asserted"}
