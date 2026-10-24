from datetime import date, datetime, time
import unittest
from zoneinfo import ZoneInfo

from db.models import User
from parsing.alerts_parser import DaySchedule, ScheduleInterval
from utils.formatting import format_graph_message, format_status_message


class FormattingTests(unittest.TestCase):
    def test_graph_groups_intervals_and_preserves_midnight_boundary(self):
        schedule = DaySchedule(
            "cherkasy", "Черкаська область", 4, 2,
            [
                ScheduleInterval(time(0), time(2), "on"),
                ScheduleInterval(time(2), time(3), "off"),
                ScheduleInterval(time(22), time(0), "on", ends_at_midnight=True),
            ],
            schedule_date=date(2026, 10, 18),
        )
        now = datetime(2026, 10, 18, 1, 0, tzinfo=ZoneInfo("Europe/Kyiv"))
        text = format_graph_message(schedule, "on", now)
        self.assertIn("Черга 4.2", text)
        self.assertIn("🟢 00:00–02:00", text)
        self.assertIn("🟢 22:00–24:00", text)
        self.assertIn("🔴 02:00–03:00", text)
        self.assertEqual(text.count("00:00–02:00"), 1)
        self.assertIn("⚡ За графіком зараз світло є", text)
        self.assertNotIn("⚡ Зараз світло є", text)
        self.assertNotIn("⚡ Зараз світла немає", text)
        self.assertIn("Найближче відключення — о 02:00", text)

    def test_graph_status_for_currently_off_state_and_next_restoration(self):
        schedule = DaySchedule(
            "cherkasy", "Черкаська область", 4, 2,
            [
                ScheduleInterval(time(0), time(8), "off"),
                ScheduleInterval(time(8), time(10), "on"),
                ScheduleInterval(time(10), time(12), "off"),
            ],
            schedule_date=date(2026, 10, 18),
        )
        now = datetime(2026, 10, 18, 7, 0, tzinfo=ZoneInfo("Europe/Kyiv"))
        text = format_graph_message(schedule, "off", now)
        self.assertIn("⚡ За графіком зараз світла немає", text)
        self.assertNotIn("⚡ Зараз світло є", text)
        self.assertNotIn("⚡ Зараз світла немає", text)
        self.assertIn("Найближче відновлення світла — о 08:00", text)

    def test_empty_graph_does_not_invent_state(self):
        schedule = DaySchedule("cherkasy", "Черкаська область", 4, 2, [], confirmed_empty=True)
        text = format_graph_message(schedule, "unknown")
        self.assertIn("Інтервали графіка відсутні", text)
        self.assertNotIn("Зараз світло", text)

    def test_status_displays_compact_queue_and_cross_midnight_quiet_hours(self):
        user = User(1, "cherkasy", "Черкаська область", 4, 2, True, "23:00", "07:00")
        text = format_status_message(user)
        self.assertIn("Черга: 4.2", text)
        self.assertIn("Тиха година: 23:00–07:00", text)


if __name__ == "__main__":
    unittest.main()
