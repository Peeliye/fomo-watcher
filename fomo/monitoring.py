"""Background RPC telemetry and read-only portfolio market marking."""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from curl_cffi import requests as cf

from .execution.rpc_pool import run_rpc_probe_cycle
from .portfolio.ledger import PortfolioLedger


DEX_CHAIN_IDS = {
    1: "ethereum", 56: "bsc", 137: "polygon",
    4663: "robinhood", 8453: "base", 1399811149: "solana",
}


def fetch_dexscreener_marks(tokens: list[dict[str, Any]], timeout_seconds: float = 8.0) -> list[dict[str, Any]]:
    """Fetch public token prices in batches; never treats a cross-chain match as valid."""
    marks: list[dict[str, Any]] = []
    captured = datetime.now(timezone.utc).isoformat()
    for start in range(0, len(tokens), 30):
        batch = tokens[start:start + 30]
        addresses = ",".join(str(item["tokenAddress"]) for item in batch)
        response = cf.get(f"https://api.dexscreener.com/latest/dex/tokens/{addresses}",
                          headers={"Accept": "application/json"}, timeout=timeout_seconds, impersonate="chrome")
        response.raise_for_status()
        pairs = response.json().get("pairs") or []
        for token in batch:
            address = str(token["tokenAddress"])
            chain = DEX_CHAIN_IDS.get(int(token["chainId"]), "")
            candidates = [pair for pair in pairs if str(pair.get("chainId") or "").casefold() == chain
                          and address.casefold() in {
                              str((pair.get("baseToken") or {}).get("address") or "").casefold(),
                              str((pair.get("quoteToken") or {}).get("address") or "").casefold(),
                          }]
            if not candidates:
                continue
            pair = max(candidates, key=lambda item: float((item.get("liquidity") or {}).get("usd") or 0))
            price = pair.get("priceUsd")
            if price is not None and float(price) > 0:
                marks.append({"chainId": token["chainId"], "tokenAddress": address,
                              "priceUsd": price, "capturedAt": captured, "source": "dexscreener"})
    return marks


class BackgroundMonitors:
    def __init__(self, project_dir: Path, cfg: dict[str, Any],
                 market_fetcher: Callable[[list[dict[str, Any]], float], list[dict[str, Any]]] = fetch_dexscreener_marks):
        self.project_dir, self.cfg, self.market_fetcher = project_dir, cfg, market_fetcher
        self.stop_event = threading.Event()
        self.threads: list[threading.Thread] = []

    def start(self) -> "BackgroundMonitors":
        rpc = self.cfg.get("rpc_pool", {})
        market = self.cfg.get("market_marks", {})
        if rpc.get("background_enabled", True):
            self._spawn("rpc-health-monitor", self._rpc_loop)
        if market.get("enabled", True):
            self._spawn("portfolio-market-monitor", self._market_loop)
        return self

    def stop(self) -> None:
        self.stop_event.set()

    def _spawn(self, name: str, target: Callable[[], None]) -> None:
        thread = threading.Thread(name=name, target=target, daemon=True)
        thread.start()
        self.threads.append(thread)

    def _rpc_loop(self) -> None:
        settings = self.cfg.get("rpc_pool", {})
        interval = max(30, int(settings.get("probe_interval_seconds", 180)))
        while not self.stop_event.is_set():
            try:
                result = run_rpc_probe_cycle(self.project_dir, self.cfg,
                                             int(settings.get("probe_samples", 3)),
                                             float(settings.get("probe_timeout_seconds", 2)))
                logging.info("RPC 自动探测完成：可达 %d，交易级健康 %d", result["reachable"], result["healthy"])
            except Exception:
                logging.exception("RPC 自动探测失败")
            self.stop_event.wait(interval)

    def _market_loop(self) -> None:
        settings = self.cfg.get("market_marks", {})
        interval = max(15, int(settings.get("refresh_seconds", 60)))
        timeout = max(1.0, float(settings.get("timeout_seconds", 8)))
        portfolio = self.cfg.get("portfolio", {})
        path = Path(str(portfolio.get("database", "data/portfolio.sqlite3")))
        if not path.is_absolute():
            path = self.project_dir / path
        while not self.stop_event.is_set():
            ledger: PortfolioLedger | None = None
            try:
                ledger = PortfolioLedger(path, str(self.cfg.get("timezone", "UTC")),
                                         str(portfolio.get("account_id", "paper-main")))
                tokens = ledger.open_position_tokens()
                marks = self.market_fetcher(tokens, timeout) if tokens else []
                updated = ledger.update_market_marks(marks)
                logging.info("持仓行情刷新完成：%d/%d 个代币已更新", updated, len(tokens))
            except Exception:
                logging.exception("持仓行情刷新失败")
            finally:
                if ledger is not None:
                    ledger.close()
            self.stop_event.wait(interval)
