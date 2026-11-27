"""The latest month's summary.csv reaches the Sheet only once it has every bank the month before had."""

import os
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest import mock

import pipelines.card as card
from config import SUMMARY_CSV_FIELDNAMES
from utils.csv_helpers import write_csv

BANK_COLUMN = [["Bank Account Name"], ["BankA"], ["BankB"], ["BankC"], ["Total"]]


class FakeSheets:
    def __init__(self, tabs: list[str]) -> None:
        self.tabs = tabs
        self.writes: list[dict] = []

    def get_sheet_id(self, _sid: str, tab: str) -> int | None:
        return 1 if tab in self.tabs else None

    def read_rows(self, _sid: str, _range: str) -> list[list[str]]:
        return BANK_COLUMN

    def batch_update_ranges(self, _sid: str, data: list[dict]) -> None:
        self.writes.extend(data)


class CardSheetSyncTest(unittest.TestCase):
    def setUp(self) -> None:
        self.pipeline = card.CardPipeline()
        self.pipeline.monthly_dir = Path(tempfile.mkdtemp()) / "monthly"
        self.sheets = FakeSheets(["2000/02", "2001/01"])

    def summary(self, month: str, amounts: dict[str, str]) -> None:
        month_dir = self.pipeline.monthly_dir / month
        month_dir.mkdir(parents=True, exist_ok=True)
        rows = [{"卡別": b, "應繳金額": a} for b, a in amounts.items()]
        write_csv(month_dir / "summary.csv", rows, SUMMARY_CSV_FIELDNAMES)

    def sync(self) -> str:
        out = StringIO()
        with (
            mock.patch.dict(os.environ, {"GOOGLE_CARD_SHEET_ID": "sheet"}),
            mock.patch.object(card, "SheetsClient", lambda: self.sheets),
            redirect_stdout(out),
        ):
            self.pipeline.sync()
        return out.getvalue()

    def test_writes_each_summary_bank_into_column_e_of_next_month_tab(self):
        self.summary("1999-12", {"BankA": "1", "BankC": "2"})
        self.summary("2000-01", {"BankA": "100", "BankC": "300"})
        self.sync()
        self.assertEqual(
            self.sheets.writes,
            [
                {"range": "'2000/02'!E2", "values": [["100"]]},
                {"range": "'2000/02'!E4", "values": [["300"]]},
            ],
        )

    def test_waits_while_a_bank_from_last_month_is_missing(self):
        self.summary("1999-12", {"BankA": "1", "BankB": "2"})
        self.summary("2000-01", {"BankA": "100"})
        self.assertIn("waiting for BankB", self.sync())
        self.assertEqual(self.sheets.writes, [])

    def test_december_statement_goes_to_january_of_next_year(self):
        self.summary("2000-12", {"BankA": "1"})
        self.sync()
        self.assertEqual(self.sheets.writes, [{"range": "'2001/01'!E2", "values": [["1"]]}])

    def test_missing_tab_writes_nothing(self):
        self.summary("2000-05", {"BankA": "1"})
        self.assertIn("tab '2000/06' does not exist", self.sync())
        self.assertEqual(self.sheets.writes, [])

    def test_bank_without_a_row_writes_nothing(self):
        self.summary("2000-01", {"BankA": "1", "BankZ": "2"})
        self.assertIn("no row for BankZ", self.sync())
        self.assertEqual(self.sheets.writes, [])

    def test_without_sheet_id_nothing_is_contacted(self):
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch.object(card, "SheetsClient") as client:
            with redirect_stdout(StringIO()):
                self.pipeline.sync()
        client.assert_not_called()


if __name__ == "__main__":
    unittest.main()
