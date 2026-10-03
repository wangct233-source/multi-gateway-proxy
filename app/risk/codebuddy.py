"""CodeBuddy（G1 国内 / G2 国际）专属风险策略。

机制来源（只读参考）：wb_accounts.py:45-117（线性退避仅网络类）、
wb_accounts.py:236-241,822-853（6004 只冷模型不冷账号）、
codebuddy-反代/2 internal/pool/cooldown.go:123-419（11102 负缓存、11140 禁用、
余额耗尽冷到次日 04:00、WAF 403 抖动）。
这些业务码是腾讯 CodeBuddy 私有语义，不适用于其他上游。
"""
from __future__ import annotations

import random
import time

from .base import Action, RiskPolicy, clamp_until


class CodebuddyPolicy(RiskPolicy):
    gateway_ids = ("a-cn", "a-intl")
    # 成长任务/事件上报的真人节奏间隔（A2 autotask.go:317-326 的 45s+抖动）。
    task_interval = 45.0

    def __init__(self):
        self._backoff = {}

    def classify(self, status: int, error_code: str, message: str, retry_after: float | None = None) -> Action:
        now = time.time()
        # 业务码优先于 HTTP 状态。
        if error_code == "6004":
            # 限流只冷模型（不冷账号），固定短窗对齐上游重置，不指数堆加。
            return Action(model_cooldown=300, reason="6004 model rate limit")
        if error_code == "11102":
            # 负缓存：该模型短窗内不再尝试。
            return Action(model_cooldown=6 * 3600, reason="11102 negative cache")
        if error_code == "11140":
            return Action(disable_account=True, account_cooldown=24 * 3600,
                          reason="11140 account disabled")
        if error_code in {"1002", "1003"} or "余额" in message or "insufficient" in message.lower():
            # 余额耗尽：冷到次日 04:00，等签到/重置解冻。
            until = clamp_until(now + 12 * 3600, latest_hour=4)
            return Action(account_cooldown=until - now, reason="insufficient balance")
        if status == 403:
            # WAF 拦截：账号+出口一起抖动冷却，短窗内别再撞。
            return Action(account_cooldown=clamp_until(now + random.uniform(300, 900)),
                          egress_cooldown=60, reason="waf 403")
        if status == 401:
            return Action(account_cooldown=15, reason="401 token invalid")
        if status == 429:
            return Action(account_cooldown=60, reason="429 rate limit")
        if status >= 500:
            return Action(reason="upstream 5xx; retryable, no cooldown")
        if status >= 400:
            return Action(account_cooldown=30, reason=f"client error {status}")
        return Action()
