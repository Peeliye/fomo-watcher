"""Persistent, validated paper-position exit policy."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


DEFAULT_EXIT_POLICY: dict[str, Any] = {
    "enabled": True,
    "stopLossPct": 25.0,
    "principalRecoveryMultiple": 2.0,
    "trailingStopPct": 25.0,
    "maxHoldingHours": 168.0,
}


class ExitPolicyError(ValueError):
    pass


def validate_exit_policy(value: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ExitPolicyError("退出策略必须是 JSON 对象")
    try:
        policy = {
            "enabled": bool(value.get("enabled", True)),
            "stopLossPct": float(value.get("stopLossPct", DEFAULT_EXIT_POLICY["stopLossPct"])),
            "principalRecoveryMultiple": float(value.get("principalRecoveryMultiple", DEFAULT_EXIT_POLICY["principalRecoveryMultiple"])),
            "trailingStopPct": float(value.get("trailingStopPct", DEFAULT_EXIT_POLICY["trailingStopPct"])),
            "maxHoldingHours": float(value.get("maxHoldingHours", DEFAULT_EXIT_POLICY["maxHoldingHours"])),
        }
    except (TypeError, ValueError) as exc:
        raise ExitPolicyError("退出策略参数必须是数字") from exc
    if not 0 < policy["stopLossPct"] < 100:
        raise ExitPolicyError("止损比例必须大于 0 且小于 100")
    if not 1 < policy["principalRecoveryMultiple"] <= 100:
        raise ExitPolicyError("出本触发倍数必须大于 1")
    if not 0 < policy["trailingStopPct"] < 100:
        raise ExitPolicyError("回撤止盈比例必须大于 0 且小于 100")
    if not 1 <= policy["maxHoldingHours"] <= 8760:
        raise ExitPolicyError("最长持仓必须在 1–8760 小时之间")
    return policy


class ExitPolicyStore:
    def __init__(self, path: str | Path, defaults: dict[str, Any] | None = None):
        self.path = Path(path)
        merged = dict(DEFAULT_EXIT_POLICY)
        if defaults:
            merged.update(defaults)
        self.defaults = validate_exit_policy(merged)

    def read(self) -> dict[str, Any]:
        if not self.path.exists():
            return dict(self.defaults)
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
            return validate_exit_policy({**self.defaults, **value})
        except (OSError, json.JSONDecodeError, ExitPolicyError):
            return dict(self.defaults)

    def write(self, value: dict[str, Any]) -> dict[str, Any]:
        policy = validate_exit_policy(value)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle, temporary = tempfile.mkstemp(prefix="exit-policy-", suffix=".json", dir=self.path.parent)
        try:
            with os.fdopen(handle, "w", encoding="utf-8") as stream:
                json.dump(policy, stream, ensure_ascii=False, indent=2)
                stream.write("\n")
            os.replace(temporary, self.path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        return policy
