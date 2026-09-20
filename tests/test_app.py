import unittest
from pathlib import Path
import tempfile

from fomo.app import Event, State, event_links, feed_events, feishu_card, is_probably_english, paper_copy_trade, position_events, render
from fomo.web.server import _paths_signature, _websocket_text_frame, build_dashboard_payload, build_shadow_payload


CFG = {
    "timezone": "Asia/Shanghai",
    "links": {"gmgn": "https://gmgn.ai/{chain}/token/{ca}"},
}


class WatcherTests(unittest.TestCase):
    def test_dashboard_websocket_frame_is_valid_text_frame(self):
        frame = _websocket_text_frame({"type": "invalidate", "keys": ["portfolio"]})
        self.assertEqual(frame[0], 0x81)
        self.assertEqual(frame[1], len(frame) - 2)
        self.assertIn(b"portfolio", frame)

    def test_dashboard_change_signature_tracks_file_updates(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "status.json"
            before = _paths_signature([path])
            path.write_text("{}", encoding="utf-8")
            after = _paths_signature([path])
            self.assertNotEqual(before, after)

    def test_dashboard_aggregates_and_returns_latest_first(self):
        with tempfile.TemporaryDirectory() as directory:
            log_path = Path(directory) / "orders.ndjson"
            log_path.write_text(
                '{"recordedAt":"2026-09-09T10:00:00Z","status":"accepted","networkId":4663,"paperBuyUsd":10}\n'
                '{"recordedAt":"2026-09-09T10:01:00Z","status":"target_trade_too_small","networkId":1399811149,"paperBuyUsd":0}\n',
                encoding="utf-8",
            )
            payload = build_dashboard_payload(log_path)
            self.assertEqual(payload["total"], 2)
            self.assertEqual(payload["accepted"], 1)
            self.assertEqual(payload["acceptedUsd"], 10)
            self.assertEqual(payload["orders"][0]["status"], "target_trade_too_small")

    def test_dashboard_aggregates_shadow_latency(self):
        with tempfile.TemporaryDirectory() as directory:
            log_path = Path(directory) / "shadow.ndjson"
            log_path.write_text(
                '{"eventId":"1","status":"eligible","decisionLatencyMs":0.2}\n'
                '{"eventId":"2","status":"not_following","decisionLatencyMs":0.4}\n',
                encoding="utf-8",
            )
            payload = build_shadow_payload(log_path)
            self.assertEqual(payload["eligible"], 1)
            self.assertEqual(payload["averageDecisionLatencyMs"], 0.3)
            self.assertEqual(payload["executions"][0]["eventId"], "2")

    def test_feed_is_strictly_filtered_to_following_ids(self):
        payload = {"feed": [
            {"id": "a", "userId": "followed", "type": "thesis_created", "body": {"comment": "keep"}},
            {"id": "b", "userId": "stranger", "type": "thesis_created", "body": {"comment": "drop"}},
            {"id": "c", "type": "thesis_created", "body": {"comment": "also drop"}},
        ]}
        events = feed_events(payload, allowed_user_ids={"followed"})
        self.assertEqual([event.original_text for event in events], ["keep"])

    def test_sidecar_event_is_strictly_filtered_to_following_ids(self):
        import json
        import tempfile
        from pathlib import Path
        from fomo.app import State, sidecar_events

        with tempfile.TemporaryDirectory() as directory:
            stream = Path(directory) / "events.ndjson"
            stream.write_text(
                json.dumps({"payload": {"id": "a", "userId": "stranger", "type": "thesis_created", "body": {"comment": "drop"}}}) + "\n"
                + json.dumps({"payload": {"id": "b", "userId": "followed", "type": "thesis_created", "body": {"comment": "keep"}}}) + "\n",
                encoding="utf-8",
            )
            state = State(str(Path(directory) / "state.sqlite3"))
            events = sidecar_events(state, {"followed"}, str(stream))
            self.assertEqual([event.original_text for event in events], ["keep"])
            state.db.close()

    def test_feed_string_comment_is_preserved_as_original(self):
        payload = {"feed": [{
            "id": "feed-1", "type": "thesis_created", "createdAt": "2026-09-09T00:00:00Z",
            "tokenAddress": "abc", "networkId": 4663,
            "body": {"ticker": "ZZZ", "userHandle": "alice", "comment": "Original call text"},
        }]}
        event = feed_events(payload)[0]
        self.assertEqual(event.original_text, "Original call text")

    def test_feed_trade_comment_is_original_fallback(self):
        payload = {"feed": [{
            "id": "feed-2", "type": "thesis_created", "createdAt": "2026-09-09T00:00:00Z",
            "body": {"ticker": "ZZZ", "userHandle": "alice"},
            "tradeComment": {"comment": "Fallback original"},
        }]}
        event = feed_events(payload)[0]
        self.assertEqual(event.original_text, "Fallback original")

    def test_realtime_top_level_trade_fields_are_preserved(self):
        payload = [{
            "id": "rt-1", "type": "swap_buy", "userId": "followed",
            "userHandle": "alice", "createdAt": "2026-09-09T00:00:00Z",
            "ticker": "MEME", "tokenAddress": "0xabc", "networkId": 4663,
            "usdAmount": 321.5, "marketCap": 2_000_000, "price": 0.02,
        }]
        event = feed_events(payload, {"followed"})[0]
        self.assertEqual(event.symbol, "MEME")
        self.assertEqual(event.amount_usd, 321.5)
        self.assertEqual(event.market_cap, 2_000_000)
        self.assertEqual(event.user_id, "followed")

    def test_trade_type_wins_over_comment_text(self):
        payload = [
            {"id": "buy-comment", "type": "single_user_buy", "userId": "followed",
             "body": {"comment": "buy annotation", "usdAmount": 10}},
            {"id": "sell-comment", "type": "swap_sell", "userId": "followed",
             "body": {"description": "sell annotation", "usdAmount": 12}},
        ]
        events = feed_events(payload, {"followed"})
        self.assertEqual([event.kind for event in events], ["buy", "sell"])
        self.assertEqual([event.original_text for event in events], ["buy annotation", "sell annotation"])

    def test_unknown_feed_type_is_dynamic_thesis_but_still_fail_closed(self):
        payload = [
            {"id": "unknown", "type": "token_deploy", "userId": "followed", "body": {"text": "deployed"}},
            {"id": "missing", "type": "token_deploy", "body": {"text": "missing actor"}},
            {"id": "stranger", "type": "token_deploy", "userId": "stranger", "body": {"text": "drop"}},
        ]
        events = feed_events(payload, {"followed"})
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].kind, "thesis")
        self.assertEqual(events[0].original_text, "deployed")

    def test_buy_event_contains_delta_and_market_cap(self):
        old = {"CA:1399811149": {"ca": "CA", "network_id": 1399811149, "amount": 10, "price": 2, "market_cap": 100000, "symbol": "MEME"}}
        new = {"CA:1399811149": {"ca": "CA", "network_id": 1399811149, "amount": 25, "price": 2, "market_cap": 100000, "symbol": "MEME"}}
        events = position_events("alice", old, new, 0.001, 1)
        self.assertEqual(events[0].kind, "buy")
        self.assertEqual(events[0].amount_usd, 30)

    def test_english_detection(self):
        self.assertTrue(is_probably_english("Strong buy, breakout soon"))
        self.assertFalse(is_probably_english("这个币可能突破"))

    def test_message_has_copyable_ca_and_link(self):
        event = Event(id="1", kind="buy", handle="alice", created_at="2026-09-08T00:00:00Z", symbol="MEME", ca="abc", network_id=1399811149, amount_usd=99, market_cap=1000, old_amount=1, new_amount=2)
        text = render(event, CFG)
        self.assertIn("CA：abc", text)
        self.assertIn("https://gmgn.ai/sol/token/abc", text)

    def test_fomo_links_use_current_plural_route_and_fomo_chain_slug(self):
        cfg = {"links": {"fomo": "https://fomo.family/tokens/{fomo_chain}/{ca}"}}
        solana = Event(id="sol", kind="buy", handle="alice", created_at="2026-09-08T00:00:00Z", ca="mint/with space", network_id=1399811149)
        bnb = Event(id="bnb", kind="buy", handle="alice", created_at="2026-09-08T00:00:00Z", ca="0xabc", network_id=56)
        self.assertEqual(event_links(solana, cfg), [("FOMO", "https://fomo.family/tokens/solana/mint%2Fwith%20space")])
        self.assertEqual(event_links(bnb, cfg), [("FOMO", "https://fomo.family/tokens/bnb/0xabc")])

    def test_english_thesis_keeps_both(self):
        event = Event(id="2", kind="thesis", handle="alice", created_at="2026-09-08T00:00:00Z", original_text="buy now", translated_text="现在买入")
        text = render(event, CFG)
        self.assertIn("中文：现在买入", text)
        self.assertIn("原文：buy now", text)

    def test_feishu_card_uses_buttons_not_raw_links(self):
        event = Event(id="3", kind="sell", handle="alice", created_at="2026-09-08T00:00:00Z", symbol="MEME", ca="abc", network_id=1399811149, amount_usd=88, market_cap=1000)
        card = feishu_card(event, CFG)
        self.assertEqual(card["msg_type"], "interactive")
        self.assertEqual(card["card"]["elements"][-1]["actions"][0]["text"]["content"], "GMGN")
        self.assertIn("MEME · SOL", card["card"]["header"]["title"]["content"])
        ca_element = card["card"]["elements"][-2]["text"]
        self.assertEqual(ca_element["tag"], "plain_text")
        self.assertEqual(ca_element["content"], "CA  abc")

    def test_paper_copy_trade_accepts_fresh_robinhood_buy(self):
        import tempfile
        from datetime import datetime, timezone
        from pathlib import Path

        with tempfile.TemporaryDirectory() as directory:
            state = State(str(Path(directory) / "state.sqlite3"))
            cfg = {
                "timezone": "Asia/Shanghai",
                "copy_trading": {
                    "enabled": True, "mode": "paper", "network_ids": [4663],
                    "event_types": ["swap_buy", "single_user_buy"],
                    "fixed_usd": 10, "min_target_buy_usd": 100,
                    "max_signal_age_seconds": 5, "min_market_cap_usd": 100_000,
                    "max_slippage_bps": 200, "per_token_limit_usd": 20,
                    "daily_limit_usd": 100, "log_path": str(Path(directory) / "orders.ndjson"),
                },
            }
            event = Event(
                id="paper-1", kind="buy", handle="alice",
                created_at=datetime.now(timezone.utc).isoformat(), symbol="MEME",
                ca="0xabc", network_id=4663, amount_usd=500, market_cap=1_000_000,
                source_type="swap_buy",
            )
            decision = paper_copy_trade(state, event, cfg)
            self.assertEqual(decision["status"], "accepted")
            self.assertEqual(decision["paperBuyUsd"], 10)
            self.assertIn("模拟买入 $10.00", event.copy_note)
            self.assertTrue((Path(directory) / "orders.ndjson").exists())
            state.db.close()

    def test_paper_copy_trade_accepts_solana_when_enabled(self):
        import tempfile
        from datetime import datetime, timezone
        from pathlib import Path

        with tempfile.TemporaryDirectory() as directory:
            state = State(str(Path(directory) / "state.sqlite3"))
            cfg = {
                "timezone": "Asia/Shanghai",
                "copy_trading": {
                    "enabled": True, "mode": "paper",
                    "network_ids": [1, 56, 4663, 8453, 1399811149],
                    "event_types": ["swap_buy"], "fixed_usd": 10,
                    "min_target_buy_usd": 100, "max_signal_age_seconds": 5,
                    "min_market_cap_usd": 100_000, "per_token_limit_usd": 20,
                    "daily_limit_usd": 100, "log_path": str(Path(directory) / "orders.ndjson"),
                },
            }
            event = Event(
                id="paper-sol-1", kind="buy", handle="alice",
                created_at=datetime.now(timezone.utc).isoformat(), symbol="SOLMEME",
                ca="So11111111111111111111111111111111111111112",
                network_id=1399811149, amount_usd=500, market_cap=1_000_000,
                source_type="swap_buy",
            )
            decision = paper_copy_trade(state, event, cfg)
            self.assertEqual(decision["status"], "accepted")
            self.assertEqual(decision["networkId"], 1399811149)
            state.db.close()


if __name__ == "__main__":
    unittest.main()
