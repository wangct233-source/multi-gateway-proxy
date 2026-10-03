"""Trae-CN（G3）专属风险策略。

机制来源（只读参考）：Trae2api-cn src/main.py:8216-8228（9074 指数退避 60s 起步
封顶 1h）、main.py:8291-8302（全局签到串行 60s 间隔）、main.py:8385-8393
（9074 后不立即查状态避免叠加限频）、auth.py:55-63（token 5 分钟缓冲）。
9074 是字节 Trae 私有限频码，不适用于其他上游。
"""
from __future__ import annotations

import time

from .base import Action, RiskPolicy, BackoffState, clamp_until


class TraePolicy(RiskPolicy):
    gateway_ids = ("b",)
    # 签到类操作的全局账号间隔（main.py:8291-8302）。
    task_interval = 60.0

    def __init__(self):
        # (account, 9074) 连续失败计数 → 指数退避；成功后由调用方 clear。
        self._backoff = BackoffState()

    def classify(self, status: int, error_code: str, message: str, retry_after: float | None = None) -> Action:
        now = time.time()
        if error_code == "9074":
            # 9074 专项：指数退避 60s×2^n 封顶 1h，且不超过当日末尾（跨日清零近似）。
            delay = self._backoff.next_delay("9074", 60, 3600)
            return Action(account_cooldown=min(delay, clamp_until(now + delay, latest_hour=0) - now),
                          reason="9074 rate limited; backoff")
        if status == 401:
            return Action(account_cooldown=15, reason="401 Cloud-IDE-JWT invalid")
        if status == 429:
            return Action(account_cooldown=120, reason="429 rate limit")
        if status == 403:
            return Action(account_cooldown=300, egress_cooldown=60, reason="403 forbidden")
        if status >= 500:
            return Action(reason="upstream 5xx; retryable, no cooldown")
        if status >= 400:
            return Action(account_cooldown=30, reason=f"client error {status}")
        return Action()

    def clear_backoff(self):
        self._backoff.clear()
