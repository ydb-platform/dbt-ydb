import pytest

from dbt.tests.adapter.basic import files
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.util import relation_from_name, run_dbt

model_incremental = """
select * from {{ source('raw', 'seed') }}
{% if is_incremental() %}
where id > 10
{% endif %}
""".strip()

config_tmp_table = """
  {{ config(materialized="incremental", primary_key="id", tmp_relation_type="table") }}
"""

config_bad_tmp_relation_type = """
  {{ config(materialized="incremental", primary_key="id", tmp_relation_type="ephemeral") }}
"""


class TestIncrementalTmpTableRelation(BaseIncremental):
    """The pre-0.0.16 path -- stage the rows in a real table -- must keep working."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental.sql": config_tmp_table + model_incremental,
            "schema.yml": files.schema_base_yml,
        }


class TestIncrementalTmpRelationCleanup:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "inc_tmp.sql": """
{{ config(materialized='incremental', primary_key='id', unique_key='id') }}
select 1l as id, 'a'u as v
"""
        }

    def test_temp_relation_is_dropped(self, project):
        run_dbt(["run"])
        run_dbt(["run"])

        tmp_relation = relation_from_name(project.adapter, "inc_tmp__dbt_tmp")
        with pytest.raises(Exception):
            project.run_sql(f"select count(*) from {tmp_relation}", fetch="one")

    def test_stale_temp_relation_does_not_break_the_run(self, project):
        # a crashed run -- or an adapter version that staged into a table -- can leave
        # an object of either kind behind under the temp name
        tmp_relation = relation_from_name(project.adapter, "inc_tmp__dbt_tmp")
        # DDL has to go through the adapter -- the test helper runs statements inside a
        # transaction, which YDB does not allow for scheme operations
        with project.adapter.connection_named("_stale_tmp"):
            project.adapter.execute(
                f"create table {tmp_relation} (id Int64 NOT NULL, v Utf8, primary key (id))"
            )

        run_dbt(["run"])


class TestIncrementalInNestedSchema:
    """A schema is a directory in YDB, so the temp relation is created, read and
    dropped by a path several levels deep."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "inc_nested.sql": """
{{ config(materialized='incremental', primary_key='id', unique_key='id', schema='sub') }}
select 1l as id, 'a'u as v
"""
        }

    @pytest.fixture(scope="class", autouse=True)
    def drop_child_schema(self, project):
        yield
        # the harness only drops the top-level test schema, and YDB refuses to remove a
        # directory that still has children
        child = project.adapter.Relation.create(
            database=project.database, schema=f"{project.test_schema}/sub"
        )
        with project.adapter.connection_named("_drop_child_schema"):
            project.adapter.drop_schema(child)

    def test_incremental_runs_in_a_nested_schema(self, project):
        run_dbt(["run"])
        run_dbt(["run"])

        model = f"`{project.test_schema}/sub/inc_nested`"
        assert project.run_sql(f"select count(*) from {model}", fetch="one")[0] == 1

        tmp_relation = f"`{project.test_schema}/sub/inc_nested__dbt_tmp`"
        with pytest.raises(Exception):
            project.run_sql(f"select count(*) from {tmp_relation}", fetch="one")


class TestInvalidTmpRelationType:
    @pytest.fixture(scope="class")
    def models(self):
        return {"inc_bad.sql": config_bad_tmp_relation_type + "select 1l as id"}

    def test_invalid_tmp_relation_type_is_rejected(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert "tmp_relation_type" in results[0].message
