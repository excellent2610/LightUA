from __future__ import annotations

import unittest
from datetime import datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup

from parsing.alerts_parser import (
    AlertsParser,
    ScheduleNotFoundError,
    _collect_target_blocks,
    _covers_full_day,
    _canonical_day_intervals,
    _extract_group_intervals,
    _extract_schedule_date,
    _extract_source_updated_at,
    DaySchedule,
    ScheduleInterval,
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

    def test_all_60_flat_groups_in_real_fixture_cover_the_full_day(self) -> None:
        _soup, text = self._read("kyiv-current.html")
        for queue in range(1, 61):
            with self.subTest(queue=queue):
                intervals = _extract_group_intervals(text, queue, 1)
                self.assertTrue(intervals)
                self.assertTrue(_covers_full_day(intervals))

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

    def test_supported_queue_and_subqueue_header_forms_parse_complete_days(self) -> None:
        day = "00:00 - 06:00 ON\n06:00 - 08:00 OFF\n08:00 - 24:00 ON"
        cases = (
            ("Група 1.1", "Група 1.2", 1, 1),
            ("Черга 1/2", "Черга 1/3", 1, 2),
            ("Група 1-3", "Група 1-4", 1, 3),
        )
        for first_header, second_header, queue, subqueue in cases:
            with self.subTest(header=first_header):
                text = f"{first_header}\n{day}\n{second_header}\n{day}"
                intervals = _extract_group_intervals(text, queue, subqueue)
                self.assertEqual(len(intervals), 3)
                self.assertTrue(_covers_full_day(intervals))

    def test_24_hour_end_is_retained_as_midnight_boundary(self) -> None:
        interval = _extract_group_intervals("Група 1\n22:00 - 24:00 OFF", 1, 1)[0]
        self.assertEqual(interval.start, time(22))
        self.assertEqual(interval.end, time(0))
        self.assertTrue(interval.ends_at_midnight)

    def test_wrapped_interval_is_split_for_full_day_validation_and_state_lookup(self) -> None:
        intervals = _extract_group_intervals(
            "Група 1\n22:00 - 02:00 OFF\n02:00 - 22:00 ON", 1, 1
        )

        self.assertEqual(len(intervals), 2)
        self.assertEqual(intervals[0].start, time(2))
        self.assertEqual(intervals[1].start, time(22))
        self.assertTrue(_covers_full_day(intervals))
        self.assertEqual(
            _canonical_day_intervals(intervals),
            [(0, 120, "off"), (120, 1320, "on"), (1320, 1440, "off")],
        )
        schedule = DaySchedule("kyiv", "Київ", 1, 1, intervals)
        self.assertEqual(schedule.state_at(datetime(2026, 10, 17, 23, 0, tzinfo=ZoneInfo("Europe/Kyiv"))), "off")
        self.assertEqual(schedule.state_at(datetime(2026, 10, 18, 1, 0, tzinfo=ZoneInfo("Europe/Kyiv"))), "off")
        self.assertEqual(schedule.state_at(datetime(2026, 10, 18, 2, 0, tzinfo=ZoneInfo("Europe/Kyiv"))), "on")

        parser = AlertsParser()
        parser._fetch_url = lambda _url: (
            "<title>Графік 17.10.2026</title><b>Група 1</b>"
            "<div>22:00 - 02:00 OFF</div><div>02:00 - 22:00 ON</div>"
        )
        parsed_schedule = parser.get_schedule("kyiv", 1, 1)
        self.assertTrue(_covers_full_day(parsed_schedule.intervals))

    def test_wrapped_intervals_do_not_make_incomplete_or_ambiguous_days_valid(self) -> None:
        incomplete = [ScheduleInterval(time(22), time(2), "off")]
        overlapping = [
            ScheduleInterval(time(22), time(2), "off"),
            ScheduleInterval(time(0), time(3), "on"),
            ScheduleInterval(time(2), time(22), "on"),
        ]
        ambiguous = [
            ScheduleInterval(time(0), time(6), "on"),
            ScheduleInterval(time(6), time(6), "off"),
            ScheduleInterval(time(6), time(0), "on", ends_at_midnight=True),
        ]

        self.assertFalse(_covers_full_day(incomplete))
        self.assertFalse(_covers_full_day(overlapping))
        self.assertFalse(_covers_full_day(ambiguous))
        self.assertFalse(_covers_full_day([]))

        invalid_start = _extract_group_intervals(
            "Група 1\n24:00 - 02:00 OFF\n02:00 - 24:00 ON", 1, 1
        )
        self.assertFalse(_covers_full_day(invalid_start))

    def test_incomplete_source_day_is_rejected_before_it_can_be_snapshotted(self) -> None:
        full_day = [
            ScheduleInterval(time(0), time(6), "on"),
            ScheduleInterval(time(6), time(8), "off"),
            ScheduleInterval(time(8), time(16), "on"),
            ScheduleInterval(time(16), time(20), "off"),
            ScheduleInterval(time(20), time(0), "on", ends_at_midnight=True),
        ]
        missing_passed_interval = [item for item in full_day if item.start != time(6)]
        self.assertTrue(_covers_full_day(full_day))
        self.assertFalse(_covers_full_day(missing_passed_interval))

        parser = AlertsParser()
        parser._fetch_url = lambda _url: (
            "<title>Графік 18.10.2026</title>"
            "<b>Група 4.2</b>"
            "<div>00:00 - 06:00 ON</div>"
            "<div>08:00 - 16:00 ON</div>"
            "<div>16:00 - 20:00 OFF</div>"
            "<div>20:00 - 24:00 ON</div>"
        )
        with self.assertRaises(ScheduleNotFoundError):
            parser.get_schedule("cherkasy", 4, 2)

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
