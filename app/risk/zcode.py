"""Zcode（G4）专属风险策略。

机制来源（只读参考）：ZcodeKnight src/proxy/risk-hold.ts:27-48（3012 按
出口 IP 计静默、模型静默窗口尊重 Retry-After）、claim/scheduler.ts:77-88,147-155
（连续错误指数退避 10min→6h 封顶、跨日 00:00 清零）、scheduler.ts:119-133
（首 tick 账号错峰防同 IP 3012）。
3012/3009 是智谱私有风控码，不适用于其他上游。
"""
from __future__ import annotations

import time

from .base import Action, RiskPolicy, BackoffState, clamp_until


class ZcodePolicy(RiskPolicy):
    gateway_ids = ("c",)
    # 领取类首跳错峰基础间隔（本网关账号天然错开；无独立 IP 时保守取 30s）。
    task_interval = 30.0

    def __init__(self):
        self._backoff = BackoffState()

    def classify(self, status: int, error_code: str, message: str,
                 retry_after: float | None = None) -> Action:
        now = time.time()
        if error_code == "3012":
            # 3012 按 IP 计：当前部署无独立出口 IP，退化为网关级（egress）静默；
            # 指数退避 10min→6h 封顶，且不超过次日 00:00（跨日清零重试语义）。
            delay = self._backoff.next_delay("3012", 600, 6 * 3600)
            return Action(egress_cooldown=min(delay, clamp_until(now + delay, latest_hour=0) - now),
                          reason="3012 ip risk hold")
        if error_code == "3009":
            # 模型级并发/配额：尊重 Retry-After，缺省 600s。
            wait = retry_after or 600
            return Action(model_cooldown=wait, reason="3009 model quota")
        if status == 401 or "login" in (error_code or "").lower() or "login_required" in message.lower():
            return Action(disable_account=True, account_cooldown=24 * 3600,
                          reason="login required; re-auth needed")
        if status == 429:
            wait = retry_after or 300
            return Action(account_cooldown=wait, reason="429 rate limit")
        if status == 403:
            return Action(account_cooldown=600, reason="403 forbidden")
        if status >= 500:
            return Action(reason="upstream 5xx; retryable, no cooldown")
        if status >= 400:
            return Action(account_cooldown=30, reason=f"client error {status}")
        return Action()

    def clear_backoff(self):
        self._backoff.clear()
