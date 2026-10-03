"""按网关隔离的风险策略骨架。

通用骨架只定义"错误 → 动作"的机制（分类、退避、封顶）；每个上游的
错误码语义、冷却时长、任务间隔写在各自的策略文件里，互不引用。
各家上游的风控体系不同（腾讯业务码 / 字节 9074 / 智谱 IP 级 3012），
参数不可互换。
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta


@dataclass
class Action:
    """对一次上游错误响应的处置动作（秒数均为相对当前时刻）。"""
    account_cooldown: float = 0
    model_cooldown: float = 0
    egress_cooldown: float = 0
    disable_account: bool = False
    reason: str = ""


@dataclass
class BackoffState:
    """按 (账号, 错误类别) 记忆连续失败次数，用于指数退避。内存态，重启清零。"""
    counts: dict = field(default_factory=dict)

    def next_delay(self, key: str, base: float, cap: float) -> float:
        n = self.counts.get(key, 0) + 1
        self.counts[key] = n
        return min(base * (2 ** min(n - 1, 16)), cap)

    def clear(self, key: str | None = None):
        if key is None:
            self.counts.clear()
        else:
            self.counts.pop(key, None)


def clamp_until(until: float, latest_hour: int | None = None) -> float:
    """冷却到期时间封顶：latest_hour=4 → 不超过次日 04:00（CodeBuddy 余额语义）；
    latest_hour=0 → 当日 24 点/次日 0 点（Zcode 跨日清零语义）。"""
    if latest_hour is None:
        return until
    now = datetime.now()
    ceiling = now.replace(hour=latest_hour, minute=0, second=0, microsecond=0)
    if ceiling <= now:
        ceiling = ceiling + timedelta(days=1)
    return min(until, ceiling.timestamp())


class RiskPolicy:
    """每个上游一份子类；字段全部属于该上游，禁止跨策略引用。"""
    gateway_ids: tuple[str, ...] = ()
    # 批量任务（签到/领取）账号之间的基础间隔秒；调用方可加 0~25% 抖动。
    task_interval: float = 5.0
    # HTTP 状态 → 默认动作由 classify 提供；这里只兜底未知错误。
    def classify(self, status: int, error_code: str, message: str, retry_after: float | None = None) -> Action:
        raise NotImplementedError

    @staticmethod
    def extract_error(body: bytes) -> tuple[str, str]:
        """从错误响应体提取 (error_code, message)。兼容 OpenAI/业务码/Anthropic 三种形态。"""
        try:
            data = json_loads_body(body)
        except Exception:
            return "", ""
        if not isinstance(data, dict):
            return "", ""
        code = data.get("code")
        if code is None:
            inner = data.get("error")
            if isinstance(inner, dict):
                code = inner.get("code") or inner.get("type")
                message = str(inner.get("message") or "")
                return str(code or ""), message
            code = data.get("error_code") or data.get("errcode")
        message = str(data.get("message") or data.get("msg") or "")
        return str(code if code is not None else ""), message


def json_loads_body(body: bytes):
    import json
    return json.loads(body.decode("utf-8", errors="replace"))


def jitter(base: float, ratio: float = 0.25, seed: float | None = None) -> float:
    """在 base 上加 ±ratio 比例的抖动（真人节奏，避免整点齐发）。"""
    import random
    return max(0.0, base * (1 + random.uniform(-ratio, ratio)))


_POLICIES: dict[str, RiskPolicy] = {}


def policy_for(gateway_id: str) -> RiskPolicy:
    """按网关取专属策略；注册表延迟导入避免环。"""
    if not _POLICIES:
        from .codebuddy import CodebuddyPolicy
        from .trae import TraePolicy
        from .zcode import ZcodePolicy
        for cls in (CodebuddyPolicy, TraePolicy, ZcodePolicy):
            for gid in cls.gateway_ids:
                _POLICIES[gid] = cls()
    return _POLICIES.get(gateway_id) or RiskPolicy()


def task_interval_for(gateway_id: str) -> float:
    return policy_for(gateway_id).task_interval
