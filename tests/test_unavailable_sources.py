"""A broker that needs a human login is skipped in one line, and later stages still run."""

import tempfile
import time
import unittest
from contextlib import redirect_stdout
from datetime import date
from io import StringIO
from pathlib import Path
from unittest import mock

from brokers import etrade
from pipelines.base import BasePipeline


def run_stages(*funcs) -> tuple[list[bool], str]:
    out = StringIO()
    with redirect_stdout(out):
        results = [
            BasePipeline.run_stage(mock.Mock(spec=BasePipeline), f"Stage {i}", f)
            for i, f in enumerate(funcs, 1)
        ]
    return results, out.getvalue()


class UnavailableSourceTest(unittest.TestCase):
    def test_etrade_without_token_off_terminal_skips_without_waiting(self):
        missing = Path(tempfile.gettempdir()) / "no-such-etrade-token.json"
        patches = {
            "ETRADE_TOKEN_FILE": missing,
            "CONSUMER_KEY": "k",
            "CONSUMER_SECRET": "s",
            "ACCOUNT_ID_KEY": "a",
        }
        later = mock.Mock()
        with (
            mock.patch.multiple(etrade, **patches),
            mock.patch.object(etrade, "_interactive_authorize") as prompt,
        ):
            broker = etrade.ETradeBroker()
            started = time.monotonic()
            results, log = run_stages(
                lambda: broker.fetch_transactions(date(2026, 1, 1)), later
            )

        self.assertLess(time.monotonic() - started, 5, "no 120 s verifier wait")
        prompt.assert_not_called()
        self.assertEqual(results, [False, True])
        later.assert_called_once()
        self.assertIn("Stage skipped: Stage 1 (E*TRADE has no valid token", log)
        self.assertNotIn("Traceback", log)

    def test_real_failures_still_print_traceback(self):
        def broken():
            raise ValueError("bad row")

        results, log = run_stages(broken)
        self.assertEqual(results, [False])
        self.assertIn("Stage failed: Stage 1", log)
        self.assertIn("ValueError: bad row", log)


if __name__ == "__main__":
    unittest.main()
