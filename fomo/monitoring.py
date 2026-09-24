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
from .portfolio.exit_policy import ExitPolicyStore


DEX_CHAIN_IDS = {
    1: "ethereum", 56: "bsc", 137: "polygon",
    4663: "robinhood", 8453: "base", 1399811149: "solana",
}

TRUSTED_QUOTE_TOKENS = {
    1: frozenset({
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
        "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
    }),
    56: frozenset({
        "0xbb4cdB9CBd36B01bD1cBaEBF2De08d9173bc095c",  # WBNB
        "0x55d398326f99059fF775485246999027B3197955",  # USDT
        "0x8AC76a51cc950d9822D68b83Fe1Ad97B32Cd580d",  # USDC
    }),
    137: frozenset({
        "0x0d500B1d8E8eD2d44f1270D6fcF7B0c2F5C03670",  # WMATIC
        "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",  # USDC
        "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",  # bridged USDC
        "0xc2132D05D31c914a87C6611C10748AaCBa5e7eE8",  # USDT
    }),
    4663: frozenset({
        "0x0bd7d308f8e1639fab988df18a8011f41eacad73",  # WETH
        "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168",  # USDG
        "0x0000000000000000000000000000000000000000",  # native ETH
    }),
    8453: frozenset({
        "0x4200000000000000000000000000000000000006",  # WETH
        "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",  # USDC
        "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",  # USDbC
    }),
    1399811149: frozenset({
        "So11111111111111111111111111111111111111112",  # WSOL
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
    }),
}
MAX_MARK_TO_REFERENCE_RATIO = 10.0


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
            chain_id = int(token["chainId"])
            chain = DEX_CHAIN_IDS.get(chain_id, "")
            trusted_quotes = {item.casefold() for item in TRUSTED_QUOTE_TOKENS.get(chain_id, ())}
            reference = float(token.get("referencePriceUsd") or 0)
            candidates = []
            for pair in pairs:
                base = str((pair.get("baseToken") or {}).get("address") or "")
                quote = str((pair.get("quoteToken") or {}).get("address") or "")
                try:
                    price = float(pair.get("priceUsd") or 0)
                    liquidity = float((pair.get("liquidity") or {}).get("usd") or 0)
                except (TypeError, ValueError):
                    continue
                ratio = max(price / reference, reference / price) if price > 0 and reference > 0 else 1.0
                if (str(pair.get("chainId") or "").casefold() != chain
                        or base.casefold() != address.casefold()
                        or quote.casefold() not in trusted_quotes
                        or price <= 0 or liquidity <= 0
                        or ratio > MAX_MARK_TO_REFERENCE_RATIO):
                    continue
                candidates.append(pair)
            if not candidates:
                continue
            pair = max(candidates, key=lambda item: float((item.get("liquidity") or {}).get("usd") or 0))
            price = pair.get("priceUsd")
            if price is not None and float(price) > 0:
                marks.append({"chainId": token["chainId"], "tokenAddress": address,
                              "priceUsd": price, "capturedAt": captured, "source": "dexscreener",
                              "pairAddress": pair.get("pairAddress"),
                              "quoteTokenAddress": (pair.get("quoteToken") or {}).get("address"),
                              "liquidityUsd": (pair.get("liquidity") or {}).get("usd")})
    return marks


class BackgroundMonitors:
    def __init__(self, project_dir: Path, cfg: dict[str, Any],
                 portfolio: PortfolioLedger | None = None,
                 market_fetcher: Callable[[list[dict[str, Any]], float], list[dict[str, Any]]] = fetch_dexscreener_marks):
        self.project_dir, self.cfg, self.market_fetcher = project_dir, cfg, market_fetcher
        self.portfolio = portfolio
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
        for thread in self.threads:
            if thread is not threading.current_thread():
                thread.join(timeout=5)

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
        configured_policy = Path(str(portfolio.get("exit_policy_path", "data/exit-policy.json")))
        policy_path = configured_policy if configured_policy.is_absolute() else self.project_dir / configured_policy
        policy_store = ExitPolicyStore(policy_path, portfolio.get("exit_strategy"))
        while not self.stop_event.is_set():
            try:
                ledger = self.portfolio
                if ledger is None:
                    self.stop_event.wait(interval)
                    continue
                tokens = ledger.open_position_tokens()
                marks = self.market_fetcher(tokens, timeout) if tokens else []
                updated = ledger.update_market_marks(marks)
                exits = ledger.evaluate_exit_rules(
                    policy_store.read(), int(portfolio.get("mark_stale_seconds", 300))
                )
                logging.info("持仓行情刷新完成：%d/%d 个代币已更新，自动退出 %d 笔", updated, len(tokens), len(exits))
            except Exception:
                logging.exception("持仓行情刷新失败")
            self.stop_event.wait(interval)
