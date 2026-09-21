from __future__ import annotations

import os
import unittest

from bip_utils import Base58Encoder
from nacl.signing import SigningKey

from fomo.execution.solana_transaction import parse_legacy_transaction, sign_legacy_transaction


class SolanaTransactionTests(unittest.TestCase):
    def test_ephemeral_legacy_signature_and_message_scope(self):
        signer = SigningKey.generate()
        signer_key = bytes(signer.verify_key)
        program_key = os.urandom(32)
        blockhash = os.urandom(32)
        message = (bytes([1, 0, 0, 2]) + signer_key + program_key + blockhash
                   + bytes([1, 1, 1, 0, 1, 42]))
        unsigned = bytes([1]) + bytes(64) + message
        parsed = parse_legacy_transaction(unsigned)
        self.assertEqual(parsed.signer, Base58Encoder.Encode(signer_key))
        self.assertEqual(parsed.recent_blockhash, Base58Encoder.Encode(blockhash))
        self.assertEqual(parsed.program_ids, (Base58Encoder.Encode(program_key),))
        signed = sign_legacy_transaction(unsigned, bytes(signer))
        signer.verify_key.verify(message, signed[1:65])
        with self.assertRaises(ValueError):
            sign_legacy_transaction(signed, bytes(signer))
        with self.assertRaises(ValueError):
            parse_legacy_transaction(unsigned + b"junk")


if __name__ == "__main__":
    unittest.main()
