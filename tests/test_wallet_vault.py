import json
import unittest

from fomo.execution.wallet_vault import SERVICE_NAME, WalletVault, derive_public_accounts, normalize_mnemonic


PHRASE = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about"


class MemoryBackend:
    def __init__(self):
        self.values: dict[tuple[str, str], str] = {}

    def get_password(self, service: str, username: str) -> str | None:
        return self.values.get((service, username))

    def set_password(self, service: str, username: str, password: str) -> None:
        self.values[(service, username)] = password

    def delete_password(self, service: str, username: str) -> None:
        del self.values[(service, username)]


class WalletVaultTests(unittest.TestCase):
    def setUp(self):
        self.backend = MemoryBackend()
        self.vault = WalletVault(self.backend)

    def test_stores_secret_only_in_credential_backend(self):
        self.vault.store("live-wallet", PHRASE, "extra secret")
        raw = self.backend.values[(SERVICE_NAME, "live-wallet")]
        payload = json.loads(raw)
        self.assertEqual(payload["mnemonic"], PHRASE)
        self.assertEqual(payload["bip39Passphrase"], "extra secret")
        self.assertEqual(self.vault.verify("live-wallet")["wordCount"], 12)

    def test_does_not_replace_without_explicit_permission(self):
        self.vault.store("live-wallet", PHRASE)
        with self.assertRaises(FileExistsError):
            self.vault.store("live-wallet", PHRASE)

    def test_rejects_bad_word_count_and_bad_name(self):
        with self.assertRaises(ValueError):
            normalize_mnemonic("only two")
        with self.assertRaises(ValueError):
            self.vault.store("../wallet", PHRASE)

    def test_delete_requires_an_existing_exact_name(self):
        self.vault.store("live-wallet", PHRASE)
        self.vault.delete("live-wallet")
        self.assertFalse(self.vault.exists("live-wallet"))
        with self.assertRaises(KeyError):
            self.vault.delete("live-wallet")

    def test_derives_stable_public_accounts_without_exposing_private_keys(self):
        self.vault.store("live-wallet", PHRASE)
        addresses = derive_public_accounts(self.vault.load("live-wallet"))
        self.assertTrue(addresses["evm"].startswith("0x"))
        self.assertEqual(len(addresses["evm"]), 42)
        self.assertGreater(len(addresses["solana"]), 31)


if __name__ == "__main__":
    unittest.main()
