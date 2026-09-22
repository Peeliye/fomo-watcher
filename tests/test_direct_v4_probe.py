import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from scripts.direct_v4_probe import _BaseHttpDiagnostic, main
from fomo.execution.rpc_pool import RpcEndpoint
from tests.test_direct_v4 import V4RpcFixture


class DirectV4ProbeTests(unittest.TestCase):
    def test_base_http_failure_metadata_has_method_tag_status_and_provider_code(self):
        class Response:
            primary_ip = "1.1.1.1"
            status_code = 200

            def json(self):
                return {"jsonrpc": "2.0", "id": 1,
                        "error": {"code": -32000, "message": "secret provider text"}}

        diagnostic = _BaseHttpDiagnostic()
        endpoint = RpcEndpoint("8453", "fixture", "primary", http_env="RPC_BASE_URL")
        with (patch.dict("os.environ", {"RPC_BASE_URL": "https://secret.invalid/key"}),
              patch("scripts.direct_v4_probe.validate_endpoint_url",
                    return_value=("https://secret.invalid/key", frozenset({"1.1.1.1"}))),
              patch("scripts.direct_v4_probe.cf.post", return_value=Response())):
            with self.assertRaisesRegex(ValueError, "rpc_method_unavailable"):
                diagnostic.request(endpoint, "eth_call", [{"to": "0x" + "11" * 20}, "0x64"])
        public = diagnostic.public(None)
        self.assertEqual(public["method"], "eth_call")
        self.assertTrue(public["blockTagged"])
        self.assertEqual(public["httpStatus"], 200)
        self.assertEqual(public["providerErrorCode"], -32000)
        self.assertNotIn("secret", str(public))

    def test_missing_configuration(self):
        stream = io.StringIO()
        with patch.dict("os.environ", {"RPC_ETHEREUM_URL": ""}), redirect_stdout(stream):
            self.assertEqual(main(), 2)
        self.assertEqual(json.loads(stream.getvalue()),
                         {"status": "未注入", "tradingReady": False})

    def test_local_quote_never_claims_simulation_or_readiness(self):
        stream = io.StringIO()
        with (patch.dict("os.environ", {"RPC_ETHEREUM_URL": "https://secret.invalid/key"}),
              patch("scripts.direct_v4_probe.FailoverJsonRpc", return_value=V4RpcFixture()),
              redirect_stdout(stream)):
            self.assertEqual(main(), 0)
        self.assertNotIn("secret.invalid", stream.getvalue())
        result = json.loads(stream.getvalue())
        self.assertFalse(result["simulationVerified"])
        self.assertFalse(result["tradingReady"])
        rows = result["evidence"]["directions"]
        self.assertEqual(rows[0]["msgValue"], rows[0]["amountIn"])
        self.assertEqual(rows[1]["msgValue"], "0")
        self.assertEqual(rows[1]["outputCurrency"], "0x" + "00" * 20)


if __name__ == "__main__":
    unittest.main()
