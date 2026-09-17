"""Profile-aware copy-trading recommendations; never authorizes execution."""

from __future__ import annotations

from typing import Any


def strategy_for_profile(profile: dict[str, Any], signal: dict[str, Any]) -> dict[str, Any]:
    style = str(profile.get("primaryStyle") or "unclassified")
    cap = float(signal.get("marketCapUsd") or 0)
    kol_buy = float(signal.get("kolBuyUsd") or signal.get("estimatedUsd") or 0)
    verified = bool(profile.get("performanceVerified"))
    blockers: list[str] = []
    if not verified:
        blockers.append("performance_not_verified")
    if cap <= 0:
        blockers.append("market_cap_missing")

    action, size, stop, hold = "observe", 0.0, None, None
    reasons: list[str] = []
    if style == "early_alpha":
        if cap <= 1_000_000:
            size = 10 if kol_buy >= 5_000 else 0
        elif cap <= 3_000_000:
            size = 10 if kol_buy >= 10_000 else 0
        else:
            size = 10 if kol_buy >= 20_000 else 0
        stop, hold = 8, 30
        action = "shadow_candidate" if size else "observe"
        reasons.append("market_cap_tier_and_kol_size")
    elif style == "pvp_verified":
        if cap < 100_000:
            blockers.append("pvp_below_100k_no_copy")
        elif kol_buy >= 20_000:
            action, size, stop, hold = "alert_only", 0.0, 6, 10
            reasons.append("large_pvp_position_alert")
    elif style == "rebound_specialist":
        action, size, stop, hold = "shadow_candidate", 5.0, 7, 20
        reasons.append("rebound_requires_lifecycle_confirmation")
    elif style == "whale_confirmation":
        action = "confirmation_only"
        blockers.append("independent_core_signal_required")
        reasons.append("whale_signal_is_not_standalone")
    else:
        blockers.append("profile_unclassified")
    if blockers:
        action = "observe" if action == "shadow_candidate" else action
        size = 0.0
    return {
        "readOnly": True, "style": style, "action": action, "shadowBuyUsd": size,
        "hardStopLossPercent": stop, "maximumHoldingMinutes": hold,
        "blockers": blockers, "reasons": reasons,
    }
