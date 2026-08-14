import pytest

from dbt.tests.adapter.incremental.test_incremental_microbatch import BaseMicrobatch
from dbt.tests.util import patch_microbatch_end_time, relation_from_name, run_dbt

# YQL has no bare datetime literals, and no CTEs to build the input rows with
input_model_sql = """
{{ config(materialized='table', primary_key='id', event_time='event_time') }}
select 1l as id, Timestamp("2020-01-01T00:00:00Z") as event_time
union all
select 2l as id, Timestamp("2020-01-02T00:00:00Z") as event_time
union all
select 3l as id, Timestamp("2020-01-03T00:00:00Z") as event_time
"""

microbatch_model_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    primary_key='id',
    unique_key='id',
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0)
) }}
select * from {{ ref('input_model') }}
"""

# the payload column is what tells a replaced row from a kept one
input_model_with_payload_sql = """
{{ config(materialized='table', primary_key='id', event_time='event_time') }}
select 1l as id, 'one'u as payload, Timestamp("2020-01-01T00:00:00Z") as event_time
union all
select 2l as id, 'two'u as payload, Timestamp("2020-01-02T00:00:00Z") as event_time
union all
select 3l as id, 'three'u as payload, Timestamp("2020-01-03T00:00:00Z") as event_time
"""


def rows(project, relation_name):
    relation = relation_from_name(project.adapter, relation_name)
    return project.run_sql(f"select id, payload from {relation} order by id", fetch="all")


class TestMicrobatch(BaseMicrobatch):
    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        return input_model_sql

    @pytest.fixture(scope="class")
    def microbatch_model_sql(self) -> str:
        return microbatch_model_sql

    @pytest.fixture(scope="class")
    def insert_two_rows_sql(self, project) -> str:
        relation = relation_from_name(project.adapter, "input_model")
        return (
            f"upsert into {relation} (id, event_time) values "
            '(4l, Timestamp("2020-01-04T00:00:00Z")), '
            '(5l, Timestamp("2020-01-05T00:00:00Z"))'
        )


class TestMicrobatchTmpTable(TestMicrobatch):
    """The same, with the batch rows staged in a table instead of a view."""

    @pytest.fixture(scope="class")
    def microbatch_model_sql(self) -> str:
        return microbatch_model_sql.replace(
            "materialized='incremental',", "materialized='incremental', tmp_relation_type='table',"
        )


class TestMicrobatchReplacesItsWindow:
    """A batch owns its window: re-running it rewrites the window from the source
    instead of merging into what is already there."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_with_payload_sql,
            "microbatch_model.sql": microbatch_model_sql,
        }

    def test_rerun_of_a_batch_rewrites_its_window(self, project):
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run"])
        assert rows(project, "microbatch_model") == [(1, "one"), (2, "two"), (3, "three")]

        input_model = relation_from_name(project.adapter, "input_model")
        # one row leaves its window, another changes inside it
        project.run_sql(f"delete from {input_model} where id = 2l")
        project.run_sql(f"update {input_model} set payload = 'third'u where id = 3l")

        run_dbt(
            [
                "run",
                "--select",
                "microbatch_model",
                "--event-time-start",
                "2020-01-02",
                "--event-time-end",
                "2020-01-04",
            ]
        )

        # the 01-02 batch lost its only row, the 01-03 batch picked up the new payload,
        # and the 01-01 batch was not part of the run at all
        assert rows(project, "microbatch_model") == [(1, "one"), (3, "third")]

    def test_full_refresh_rebuilds_every_batch(self, project):
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run", "--full-refresh"])

        # the run rebuilt the input model too, so every batch is back to its first state
        assert rows(project, "microbatch_model") == [(1, "one"), (2, "two"), (3, "three")]


class TestMicrobatchColumnStore(TestMicrobatch):
    """A column-oriented target -- the store the strategy exists for.

    Staged in a table rather than the default view: reading the row-oriented input
    through a view leaves it locked once the batch writes into a column-oriented target,
    and dropping it -- which the next run does -- then never finishes (YDB, as of today).
    """

    @pytest.fixture(scope="class")
    def microbatch_model_sql(self) -> str:
        return microbatch_model_sql.replace(
            "materialized='incremental',",
            "materialized='incremental', store_type='column', tmp_relation_type='table',",
        )

