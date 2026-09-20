from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from fomo.web.wallet_management import WalletManagementError, WalletManagementStore


EVM_ADDRESS = "0x1111111111111111111111111111111111111111"
SOLANA_ADDRESS = "498g1rVnFcnjBjpfw1xyqA1WvgQXUU8RWuELjxkjAayQ"


class WalletManagementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.registry = root / "wallet-registry.json"
        self.watchlist = root / "watch-wallets.json"
        self.audit = root / "audit.ndjson"
        self.registry.write_text('{"version":1,"wallets":[]}', encoding="utf-8")
        self.store = WalletManagementStore(self.registry, self.watchlist, self.audit)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_kol_wallet_is_validated_and_persisted(self) -> None:
        result = self.store.mutate({
            "action": "upsert_kol",
            "entry": {
                "kolId": "kol-1", "handle": "alpha", "address": EVM_ADDRESS,
                "chainIds": ["1", "8453"], "confidence": 0.9,
                "evidenceType": "public-profile", "evidenceReference": "https://example.test/alpha",
                "expiresAt": "2030-01-01T00:00:00Z", "status": "active",
            },
        })
        self.assertTrue(result["ok"])
        saved = json.loads(self.registry.read_text(encoding="utf-8"))
        self.assertEqual(saved["wallets"][0]["address"], EVM_ADDRESS)
        self.assertTrue(saved["wallets"][0]["entryId"].startswith("kol_"))
        self.assertEqual(len(self.audit.read_text(encoding="utf-8").splitlines()), 1)

    def test_kol_wallet_requires_real_platform_id(self) -> None:
        with self.assertRaisesRegex(WalletManagementError, "kolId"):
            self.store.mutate({
                "action": "upsert_kol",
                "entry": {
                    "handle": "Alpha", "address": EVM_ADDRESS, "chainIds": ["1"],
                    "evidenceType": "public-profile", "evidenceReference": "https://example.test/alpha",
                    "expiresAt": "2030-01-01T00:00:00Z",
                },
            })


    def test_mixed_chain_families_are_rejected(self) -> None:
        with self.assertRaisesRegex(WalletManagementError, "不能混合"):
            self.store.mutate({
                "action": "upsert_watch",
                "wallet": {"name": "mixed", "address": EVM_ADDRESS, "chainIds": ["1", "1399811149"]},
            })

    def test_secret_fields_are_rejected(self) -> None:
        with self.assertRaisesRegex(WalletManagementError, "不能提交私钥"):
            self.store.mutate({
                "action": "upsert_watch",
                "wallet": {"name": "unsafe", "address": EVM_ADDRESS, "chainIds": ["1"], "privateKey": "never"},
            })

    def test_csv_import_is_atomic_and_duplicate_safe(self) -> None:
        csv_data = (
            "name,address,chainIds,tags,notifyBuy,notifySell\n"
            f"sol whale,{SOLANA_ADDRESS},1399811149,whale|sol,true,true\n"
        )
        result = self.store.mutate({"action": "import_watch", "format": "csv", "data": csv_data})
        self.assertEqual(result["imported"], 1)
        with self.assertRaisesRegex(WalletManagementError, "重复地址"):
            self.store.mutate({"action": "import_watch", "format": "csv", "data": csv_data})
        saved = json.loads(self.watchlist.read_text(encoding="utf-8"))
        self.assertEqual(len(saved["wallets"]), 1)

    def test_watch_wallet_can_be_paused_and_deleted(self) -> None:
        created = self.store.mutate({
            "action": "upsert_watch",
            "wallet": {"name": "base whale", "address": EVM_ADDRESS, "chainIds": ["8453"]},
        })["record"]
        paused = self.store.mutate({"action": "set_watch_status", "id": created["id"], "status": "paused"})
        self.assertEqual(paused["record"]["status"], "paused")
        self.store.mutate({"action": "delete_watch", "id": created["id"]})
        self.assertEqual(self.store.snapshot()["watchWallets"], [])


if __name__ == "__main__":
    unittest.main()
