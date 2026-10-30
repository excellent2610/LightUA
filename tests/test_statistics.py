from __future__ import annotations

import asyncio
import csv
import os
import statistics
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Mapping
from unittest.mock import AsyncMock, patch

from lightua_statistics.code import regions
from lightua_statistics.code.regions import (
    fetch_user_statistics,
    generate_regions_csv,
    main,
    write_regions_csv,
)
from lightua_statistics.code.users import (
    REGION_STATISTICS_QUERY,
    UNKNOWN_REGION,
    UserStatistics,
    aggregate_user_rows,
    load_user_statistics,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def aggregate_row(region_name: object, users: object, active: object = 0) -> Mapping[str, object]:
    return {
        "region_name": region_name,
        "user_count": users,
        "active_count": active,
    }


class FakeTransaction:
    def __init__(self) -> None:
        self.readonly: bool | None = None

    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, *_: object) -> bool:
        return False


class FakeConnection:
    def __init__(
        self,
        rows: list[Mapping[str, object]] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.rows = rows or []
        self.error = error
        self.query: str | None = None
        self.transaction_context = FakeTransaction()

    def transaction(self, *, readonly: bool = False) -> FakeTransaction:
        self.transaction_context.readonly = readonly
        return self.transaction_context

    async def fetch(self, query: str) -> list[Mapping[str, object]]:
        self.query = query
        if self.error is not None:
            raise self.error
        return self.rows


class FakeAcquire:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    async def __aenter__(self) -> FakeConnection:
        return self.connection

    async def __aexit__(self, *_: object) -> bool:
        return False


class FakePool:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.closed = False

    def acquire(self) -> FakeAcquire:
        return FakeAcquire(self.connection)

    async def close(self) -> None:
        self.closed = True


class StatisticsTests(unittest.TestCase):
    def test_project_package_does_not_shadow_python_stdlib_statistics(self):
        self.assertEqual(statistics.mean([1, 2, 3]), 2)
        self.assertEqual(statistics.NormalDist(0, 1).mean, 0)
        self.assertNotEqual(Path(statistics.__file__).resolve().parent, PROJECT_ROOT)

    def test_user_and_active_counts_are_aggregated(self):
        stats = aggregate_user_rows(
            [
                aggregate_row("Черкаська область", 4, 2),
                aggregate_row("Київська", 8, 3),
            ]
        )

        self.assertEqual(stats.total_users, 12)
        self.assertEqual(stats.active_users_24h, 5)
        self.assertEqual(
            stats.users_by_region,
            (("Київська область", 8), ("Черкаська область", 4)),
        )

    def test_empty_database_returns_zero_statistics(self):
        stats = aggregate_user_rows([])

        self.assertEqual(stats, UserStatistics(0, 0, ()))

    def test_missing_and_blank_regions_are_grouped_as_unknown(self):
        stats = aggregate_user_rows(
            [
                aggregate_row(None, 2, 1),
                aggregate_row("", 3),
                aggregate_row("   ", 1),
            ]
        )

        self.assertEqual(stats.total_users, 6)
        self.assertEqual(stats.users_by_region, ((UNKNOWN_REGION, 6),))

    def test_unknown_and_misspelled_regions_are_not_guessed(self):
        stats = aggregate_user_rows(
            [
                aggregate_row("Невідома область", 2),
                aggregate_row("Черкаськаа область", 1),
            ]
        )

        self.assertEqual(stats.total_users, 3)
        self.assertEqual(stats.users_by_region, ((UNKNOWN_REGION, 3),))

    def test_case_and_supported_name_formats_merge(self):
        stats = aggregate_user_rows(
            [
                aggregate_row("Черкаська область", 2),
                aggregate_row("черкаська", 3),
                aggregate_row("ЧЕРКАСЬКА ОБЛ.", 4),
                aggregate_row("м. Київ", 1),
                aggregate_row("МІСТО КИЇВ", 2),
            ]
        )

        self.assertEqual(
            stats.users_by_region,
            (("Черкаська область", 9), ("м. Київ", 3)),
        )
        self.assertEqual(stats.total_users, 12)

    def test_all_users_are_counted_once_including_unknown_regions(self):
        stats = aggregate_user_rows(
            [
                aggregate_row(None, 2),
                aggregate_row("", 3),
                aggregate_row("Невідома область", 4),
                aggregate_row("Черкаська область", 5),
                aggregate_row("черкаська", 7),
            ]
        )

        self.assertEqual(stats.total_users, 21)
        self.assertEqual(
            stats.users_by_region,
            (("Черкаська область", 12), (UNKNOWN_REGION, 9)),
        )
        self.assertEqual(sum(count for _, count in stats.users_by_region), stats.total_users)

    def test_grouping_query_returns_only_aggregates_without_personal_data(self):
        class FakeConnection:
            async def fetch(self, query: str):
                self.query = query
                return [
                    aggregate_row("Львівська область", 12, 4),
                    aggregate_row(None, 1, 0),
                ]

        connection = FakeConnection()
        stats = asyncio.run(load_user_statistics(connection))

        self.assertEqual(stats.total_users, 13)
        self.assertEqual(stats.active_users_24h, 4)
        self.assertIn("GROUP BY region_name", connection.query)
        self.assertIn("last_active", connection.query)
        self.assertNotIn("telegram_id", connection.query)
        self.assertNotIn("username", connection.query)

    def test_missing_database_url_is_reported(self):
        with (
            patch.dict("os.environ", {"DATABASE_URL": ""}),
            patch("lightua_statistics.code.regions.load_dotenv"),
        ):
            with self.assertRaisesRegex(RuntimeError, "DATABASE_URL is required"):
                asyncio.run(fetch_user_statistics())

    def test_connection_error_and_timeout_propagate(self):
        for error in (OSError("database unavailable"), TimeoutError("connection timed out")):
            with self.subTest(error=type(error).__name__):
                with (
                    patch.dict("os.environ", {"DATABASE_URL": "postgresql://test.invalid/db"}),
                    patch("lightua_statistics.code.regions.load_dotenv"),
                    patch(
                        "lightua_statistics.code.regions.asyncpg.create_pool",
                        new=AsyncMock(side_effect=error),
                    ),
                ):
                    with self.assertRaises(type(error)):
                        asyncio.run(fetch_user_statistics())

    def test_query_is_read_only_and_pool_closes_after_success(self):
        connection = FakeConnection(
            [
                aggregate_row("Черкаська область", 4, 2),
                aggregate_row("Київська область", 3, 1),
            ]
        )
        pool = FakePool(connection)
        create_pool = AsyncMock(return_value=pool)

        with (
            patch.dict("os.environ", {"DATABASE_URL": "postgresql://test.invalid/db"}),
            patch("lightua_statistics.code.regions.load_dotenv"),
            patch(
                "lightua_statistics.code.regions.asyncpg.create_pool",
                new=create_pool,
            ),
        ):
            stats = asyncio.run(fetch_user_statistics())

        create_pool.assert_awaited_once_with("postgresql://test.invalid/db", min_size=1, max_size=2)
        self.assertTrue(connection.transaction_context.readonly)
        self.assertEqual(connection.query, REGION_STATISTICS_QUERY)
        self.assertEqual(stats, UserStatistics(7, 3, (("Київська область", 3), ("Черкаська область", 4))))
        self.assertTrue(pool.closed)

    def test_sql_error_propagates_and_pool_closes(self):
        connection = FakeConnection(error=RuntimeError("query failed"))
        pool = FakePool(connection)

        with (
            patch.dict("os.environ", {"DATABASE_URL": "postgresql://test.invalid/db"}),
            patch("lightua_statistics.code.regions.load_dotenv"),
            patch(
                "lightua_statistics.code.regions.asyncpg.create_pool",
                new=AsyncMock(return_value=pool),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "query failed"):
                asyncio.run(fetch_user_statistics())

        self.assertTrue(connection.transaction_context.readonly)
        self.assertTrue(pool.closed)

    def test_cli_error_log_does_not_include_database_url(self):
        secret_url = "postgresql://user:secret@example.invalid/database"
        def fail_run(coroutine):
            coroutine.close()
            raise RuntimeError(secret_url)

        with (
            patch("lightua_statistics.code.regions.asyncio.run", side_effect=fail_run),
            self.assertLogs("lightua_statistics.code.regions", level="ERROR") as captured,
        ):
            with self.assertRaises(SystemExit) as result:
                main()

        self.assertEqual(result.exception.code, 1)
        self.assertNotIn(secret_url, "\n".join(captured.output))

    def test_default_csv_destination_is_independent_of_current_directory(self):
        stats = aggregate_user_rows([aggregate_row("Сумська область", 2)])
        expected_default = (
            Path(regions.__file__).resolve().parents[2]
            / "lightua_statistics"
            / "tables"
            / "regions.csv"
        )
        self.assertEqual(regions.DEFAULT_OUTPUT_PATH, expected_default)

        with tempfile.TemporaryDirectory() as directory:
            output_path = Path(directory) / "project" / "statistics" / "tables" / "regions.csv"
            previous_directory = Path.cwd()
            try:
                os.chdir(directory)
                with (
                    patch.object(regions, "DEFAULT_OUTPUT_PATH", output_path),
                    patch(
                        "lightua_statistics.code.regions.fetch_user_statistics",
                        new=AsyncMock(return_value=stats),
                    ),
                ):
                    result = asyncio.run(generate_regions_csv())
            finally:
                os.chdir(previous_directory)

            with output_path.open(encoding="utf-8", newline="") as file:
                rows = list(csv.reader(file))

        self.assertEqual(result, stats)
        self.assertEqual(rows[-1], ["Сумська область", "2"])

    def test_module_resolves_from_root_and_external_directory_with_project_path(self):
        environment = os.environ.copy()
        environment.update(
            {
                "DATABASE_URL": "",
                "PYTHONDONTWRITEBYTECODE": "1",
            }
        )
        with tempfile.TemporaryDirectory() as directory:
            for working_directory, python_path in (
                (PROJECT_ROOT, None),
                (Path(directory), str(PROJECT_ROOT)),
            ):
                with self.subTest(working_directory=working_directory):
                    run_environment = environment.copy()
                    if python_path is not None:
                        run_environment["PYTHONPATH"] = python_path
                    result = subprocess.run(
                        [sys.executable, "-m", "lightua_statistics.code.regions"],
                        cwd=working_directory,
                        env=run_environment,
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    self.assertEqual(result.returncode, 1)
                    self.assertIn("DATABASE_URL is required", result.stderr)

    def test_csv_is_utf8_stable_and_repeatable(self):
        stats = aggregate_user_rows(
            [
                aggregate_row("Полтавська область", 3),
                aggregate_row("Черкаська", 4),
                aggregate_row(None, 2),
            ]
        )

        with tempfile.TemporaryDirectory() as directory:
            output_path = Path(directory) / "missing" / "regions.csv"
            write_regions_csv(stats, output_path)
            first_content = output_path.read_bytes()
            write_regions_csv(stats, output_path)
            second_content = output_path.read_bytes()

            self.assertEqual(first_content, second_content)
            self.assertIn("Черкаська область".encode("utf-8"), first_content)
            with output_path.open(encoding="utf-8", newline="") as file:
                rows = list(csv.reader(file))

        self.assertEqual(
            rows,
            [
                ["Область", "Користувачів"],
                ["Полтавська область", "3"],
                ["Черкаська область", "4"],
                [UNKNOWN_REGION, "2"],
            ],
        )
        self.assertEqual(sum(int(row[1]) for row in rows[1:]), stats.total_users)

    def test_empty_statistics_write_header_only_csv(self):
        with tempfile.TemporaryDirectory() as directory:
            output_path = Path(directory) / "regions.csv"
            write_regions_csv(UserStatistics(0, 0, ()), output_path)

            with output_path.open(encoding="utf-8", newline="") as file:
                rows = list(csv.reader(file))

        self.assertEqual(rows, [["Область", "Користувачів"]])

    def test_csv_contains_aggregates_only_and_no_personal_data(self):
        stats = aggregate_user_rows([aggregate_row("Львівська область", 12)])

        with tempfile.TemporaryDirectory() as directory:
            output_path = Path(directory) / "regions.csv"
            write_regions_csv(stats, output_path)
            content = output_path.read_text(encoding="utf-8")

        self.assertNotIn("8608243726", content)
        self.assertNotIn("@example", content)
        self.assertNotIn("username", content)

    def test_concurrent_generations_leave_one_complete_csv(self):
        stats = aggregate_user_rows([aggregate_row("Одеська область", 3)])

        with tempfile.TemporaryDirectory() as directory:
            output_path = Path(directory) / "regions.csv"
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(write_regions_csv, stats, output_path)
                    for _ in range(2)
                ]
                for future in futures:
                    future.result()

            with output_path.open(encoding="utf-8", newline="") as file:
                rows = list(csv.reader(file))
            leftovers = list(Path(directory).glob(".*.tmp"))

        self.assertEqual(rows, [["Область", "Користувачів"], ["Одеська область", "3"]])
        self.assertEqual(leftovers, [])

    def test_existing_csv_survives_write_error(self):
        with tempfile.TemporaryDirectory() as directory:
            output_path = Path(directory) / "regions.csv"
            previous_content = "old,complete\n"
            output_path.write_text(previous_content, encoding="utf-8")
            stats = UserStatistics(1, 0, (("Черкаська область", 1),))

            with patch(
                "lightua_statistics.code.regions.csv.writer",
                side_effect=OSError("simulated write failure"),
            ):
                with self.assertRaisesRegex(OSError, "simulated write failure"):
                    write_regions_csv(stats, output_path)

            self.assertEqual(output_path.read_text(encoding="utf-8"), previous_content)
            self.assertEqual(list(Path(directory).glob(".*.tmp")), [])

    def test_inconsistent_statistics_are_not_written(self):
        with tempfile.TemporaryDirectory() as directory:
            output_path = Path(directory) / "regions.csv"
            previous_content = "old,complete\n"
            output_path.write_text(previous_content, encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "must sum to total_users"):
                write_regions_csv(
                    UserStatistics(2, 0, (("Черкаська область", 1),)),
                    output_path,
                )

            self.assertEqual(output_path.read_text(encoding="utf-8"), previous_content)


if __name__ == "__main__":
    unittest.main()
