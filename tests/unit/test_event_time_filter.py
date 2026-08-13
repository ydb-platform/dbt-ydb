import datetime

import pytest
from dbt.adapters.base.relation import EventTimeFilter
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.ydb.literals import timestamp_literal
from dbt.adapters.ydb.relation import YDBRelation


def relation_with_filter(**filter_kwargs):
    return YDBRelation.create(
        schema="schema",
        identifier="events",
        event_time_filter=EventTimeFilter(**filter_kwargs),
    )


def test_timestamp_literal_renders_utc_iso_text():
    assert (
        timestamp_literal(datetime.datetime(2020, 1, 2, 3, 4, 5))
        == 'Timestamp("2020-01-02T03:04:05Z")'
    )


def test_timestamp_literal_normalizes_timezone_aware_values():
    value = datetime.datetime(
        2020, 1, 2, 6, 4, 5, tzinfo=datetime.timezone(datetime.timedelta(hours=3))
    )

    assert timestamp_literal(value) == 'Timestamp("2020-01-02T03:04:05Z")'


def test_timestamp_literal_keeps_microseconds():
    assert (
        timestamp_literal(datetime.datetime(2020, 1, 2, 3, 4, 5, 6))
        == 'Timestamp("2020-01-02T03:04:05.000006Z")'
    )


def test_timestamp_literal_reads_dates_and_iso_text():
    assert timestamp_literal(datetime.date(2020, 1, 2)) == 'Timestamp("2020-01-02T00:00:00Z")'
    # the text dbt itself renders a batch bound as
    assert timestamp_literal("2020-01-02 00:00:00+00:00") == 'Timestamp("2020-01-02T00:00:00Z")'
    assert timestamp_literal("2020-01-02T00:00:00Z") == 'Timestamp("2020-01-02T00:00:00Z")'


def test_timestamp_literal_rejects_what_it_cannot_read():
    with pytest.raises(DbtRuntimeError):
        timestamp_literal("last tuesday")


def test_event_time_window_is_rendered_with_yql_literals():
    relation = relation_with_filter(
        field_name="created_at",
        start=datetime.datetime(2020, 1, 2, tzinfo=datetime.timezone.utc),
        end=datetime.datetime(2020, 1, 3, tzinfo=datetime.timezone.utc),
    )

    assert str(relation) == (
        "(select * from `schema/events` "
        'where created_at >= Timestamp("2020-01-02T00:00:00Z") '
        'and created_at < Timestamp("2020-01-03T00:00:00Z"))'
        " _dbt_et_filter_subq_events"
    )


def test_an_open_ended_window_renders_one_bound():
    relation = relation_with_filter(
        field_name="created_at",
        end=datetime.datetime(2020, 1, 3, tzinfo=datetime.timezone.utc),
    )

    assert 'where created_at < Timestamp("2020-01-03T00:00:00Z")' in str(relation)


def test_no_bounds_leaves_the_relation_alone():
    relation = relation_with_filter(field_name="created_at")

    assert str(relation) == "`schema/events`"
