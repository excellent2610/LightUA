from __future__ import annotations

import unittest
from datetime import time
from pathlib import Path

from bs4 import BeautifulSoup

from parsing.alerts_parser import (
    AlertsParser,
    ScheduleNotFoundError,
    _collect_target_blocks,
    _extract_group_intervals,
    _extract_schedule_date,
    _extract_source_updated_at,
)


FIXTURES = Path(__file__).parent / "fixtures"


class AlertsParserFixtureTests(unittest.TestCase):
    def _read(self, name: str) -> tuple[BeautifulSoup, str]:
        soup = BeautifulSoup((FIXTURES / name).read_text(encoding="utf-8"), "html.parser")
        return soup, soup.get_text("\n", strip=True)

    def test_current_real_page_parses_flat_group_1_and_group_2(self) -> None:
        soup, text = self._read("kyiv-current.html")
        group1 = _extract_group_intervals(text, 1, 1)
        group2 = _extract_group_intervals(text, 2, 1)
        self.assertTrue(group1)
        self.assertTrue(group2)
        self.assertEqual(group1[0].start, time(0))
        self.assertEqual(group2[0].start, time(0))
        self.assertEqual(len(_collect_target_blocks(text.splitlines(), 1, 1)), 1)
        self.assertEqual(len(_collect_target_blocks(text.splitlines(), 2, 1)), 1)
        self.assertEqual(
            _extract_schedule_date(soup, "https://alerts.org.ua/kyiv/").isoformat(), "2026-10-17"
        )
        # The currently published page fixture has no explicit source update time.
        self.assertIsNone(_extract_source_updated_at(soup))

    def test_dated_archive_fixture_and_update_timestamp(self) -> None:
        soup, text = self._read("kyiv-2026-10-14.html")
        self.assertTrue(_extract_group_intervals(text, 1, 1))
        self.assertEqual(
            _extract_schedule_date(soup, "https://alerts.org.ua/kyiv/2026-10-14.html").isoformat(),
            "2026-10-14",
        )
        self.assertEqual(_extract_source_updated_at(soup).isoformat(), "2026-10-13T21:48:00+03:00")

    def test_split_subqueues_and_flat_headers_are_supported(self) -> None:
        text = "Група 1.1\n07:00 - 08:00 OFF\nГрупа 1.2\n09:00 - 10:00 OFF\nГрупа 2\n11:00 - 12:00 OFF"
        self.assertEqual(len(_extract_group_intervals(text, 1, 1)), 1)
        self.assertEqual(_extract_group_intervals(text, 1, 1)[0].start, time(7))
        self.assertEqual(_extract_group_intervals(text, 1, 2)[0].start, time(9))
        self.assertEqual(_extract_group_intervals(text, 2, 1)[0].start, time(11))

    def test_24_hour_end_is_retained_as_midnight_boundary(self) -> None:
        interval = _extract_group_intervals("Група 1\n22:00 - 24:00 OFF", 1, 1)[0]
        self.assertEqual(interval.start, time(22))
        self.assertEqual(interval.end, time(0))
        self.assertTrue(interval.ends_at_midnight)

    def test_invalid_or_missing_dates_are_not_invented(self) -> None:
        soup = BeautifulSoup("<title>Графік</title><h1>Графік, 31.02.2026</h1>", "html.parser")
        self.assertIsNone(_extract_schedule_date(soup, "https://alerts.org.ua/kyiv/"))

    def test_only_explicit_source_text_confirms_an_empty_schedule(self) -> None:
        parser = AlertsParser()
        parser._fetch_url = lambda _url: (
            "<title>Графік 17.10.2026</title><h1>Графік на 17.10.2026</h1>"
            "<p>Сьогодні відключень не буде</p>"
        )
        schedule = parser.get_schedule("kyiv", 1, 1)
        self.assertEqual(schedule.intervals, [])
        self.assertTrue(schedule.confirmed_empty)

        parser._fetch_url = lambda _url: "<title>Графік 17.10.2026</title><h1>Графік</h1>"
        with self.assertRaises(ScheduleNotFoundError):
            parser.get_schedule("kyiv", 1, 1)


if __name__ == "__main__":
    unittest.main()
