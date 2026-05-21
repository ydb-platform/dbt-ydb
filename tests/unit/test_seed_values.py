import datetime
import io

import agate
from dbt_common.clients.agate_helper import DEFAULT_TYPE_TESTER

from dbt.adapters.ydb.impl import YDBAdapter


def table_from_csv(csv):
    return agate.Table.from_csv(io.StringIO(csv), column_types=DEFAULT_TYPE_TESTER)


def test_seed_type_conversion_uses_ydb_types():
    table = table_from_csv(
        "id,amount,flag,created_on,created_at,name\n"
        "1,1.25,true,2024-01-02,2024-01-02T03:04:05,Alice\n"
    )

    assert YDBAdapter.convert_type(table, 0) == "Int64"
    assert YDBAdapter.convert_type(table, 1) == "Double"
    assert YDBAdapter.convert_type(table, 2) == "Bool"
    assert YDBAdapter.convert_type(table, 3) == "Date"
    assert YDBAdapter.convert_type(table, 4) == "DateTime"
    assert YDBAdapter.convert_type(table, 5) == "Utf8"


def test_prepare_insert_values_serializes_supported_agate_types():
    table = table_from_csv(
        'id,amount,flag,empty,created_on,created_at,name\n'
        '1,1.25,true,,2024-01-02,2024-01-02T03:04:05,"quote "" and\n'
        'newline"\n'
    )

    assert YDBAdapter.prepare_insert_values_from_csv(None, table) == (
        '(1, 1.25, true, NULL, Date("2024-01-02"), '
        'DateTime("2024-01-02T03:04:05Z"), "quote \\" and\\nnewline")'
    )


def test_prepare_insert_values_normalizes_timezone_aware_datetimes_to_utc():
    table = agate.Table(
        [[datetime.datetime(2024, 1, 2, 6, 4, 5, tzinfo=datetime.timezone(datetime.timedelta(hours=3)))]],
        column_names=["created_at"],
        column_types=[agate.DateTime()],
    )

    assert (
        YDBAdapter.prepare_insert_values_from_csv(None, table)
        == '(DateTime("2024-01-02T03:04:05Z"))'
    )


def test_prepare_insert_values_serializes_timedelta_as_interval():
    table = agate.Table(
        [[datetime.timedelta(days=1, seconds=2, microseconds=3)]],
        column_names=["duration"],
        column_types=[agate.TimeDelta()],
    )

    assert (
        YDBAdapter.prepare_insert_values_from_csv(None, table)
        == "(DateTime::IntervalFromMicroseconds(86402000003))"
    )
