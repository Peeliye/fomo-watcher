"""Runtime capability registry. Configuration strings never imply readiness."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class CapabilityStatus:
    name: str
    implemented: bool
    ready: bool
    reason: str
    evidence: dict[str, Any]


class SelfCheckingCapability(Protocol):
    def self_check(self) -> CapabilityStatus: ...


class CapabilityRegistry:
    REQUIRED = (
        "quote_adapter", "transaction_builder", "transaction_simulator", "signer", "broadcaster",
        "nonce_blockhash_manager", "receipt_tracker", "transaction_parser",
    )

    def __init__(self) -> None:
        self._items: dict[str, SelfCheckingCapability] = {}

    def register(self, name: str, capability: SelfCheckingCapability) -> None:
        if name not in self.REQUIRED:
            raise ValueError(f"unknown execution capability: {name}")
        self._items[name] = capability

    def status(self) -> dict[str, Any]:
        checks: dict[str, dict[str, Any]] = {}
        for name in self.REQUIRED:
            capability = self._items.get(name)
            try:
                result = (capability.self_check() if capability is not None
                          else CapabilityStatus(name, False, False, "adapter_not_registered", {}))
            except Exception as error:
                result = CapabilityStatus(name, False, False, "self_check_failed", {"errorType": type(error).__name__})
            if not isinstance(result, CapabilityStatus):
                result = CapabilityStatus(name, False, False, "self_check_invalid_result", {})
            if result.name != name:
                result = CapabilityStatus(name, result.implemented, False, "self_check_name_mismatch", {})
            checks[name] = asdict(result)
        return {"ready": all(item["implemented"] and item["ready"] for item in checks.values()),
                "capabilities": checks}


class UnavailableCapability:
    def __init__(self, name: str, reason: str = "not_implemented") -> None:
        self.name = name
        self.reason = reason

    def self_check(self) -> CapabilityStatus:
        return CapabilityStatus(self.name, False, False, self.reason, {})


class FakeSignerCapability:
    """Test/dev placeholder that is deliberately never ready."""

    def self_check(self) -> CapabilityStatus:
        return CapabilityStatus("signer", False, False, "fake_signer_forbidden", {"backend": "fake"})


DEFAULT_CAPABILITY_REGISTRY = CapabilityRegistry()
