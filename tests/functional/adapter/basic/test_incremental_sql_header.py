import pytest

from dbt.tests.util import run_dbt, write_file

# `tx: 99` can never be a valid transaction id, so whichever statement this header
# is attached to fails to compile with "Invalid tx id: 99". That makes it a precise
# probe for *which* statement a header actually ends up on.
BAD_PRAGMA = 'PRAGMA ydb.OverridePlanner = @@ [ { "tx": 99, "stage": 0, "tasks": 1 } ] @@;'

MODEL_TEMPLATE = """
{{{{ config(
    materialized='incremental',
    primary_key='id',
    unique_key='id'{extra}
) }}}}
select 1l as id, 'a'u as v
"""


def model_sql(**configs):
    extra = "".join(f",\n    {key}='{value}'" for key, value in configs.items())
    return MODEL_TEMPLATE.format(extra=extra)


class TestIncrementalMergeSqlHeader:
    @pytest.fixture(scope="class")
    def models(self):
        return {"inc_header.sql": model_sql(merge_sql_header=BAD_PRAGMA)}

    def test_merge_sql_header_only_reaches_the_upsert(self, project):
        # the first run is a plain CREATE TABLE AS, there is no merge statement yet
        run_dbt(["run"])

        # the second run goes through the UPSERT, which is where `merge_sql_header` lands
        results = run_dbt(["run"], expect_pass=False)
        assert "Invalid tx id: 99" in results[0].message


class TestIncrementalSqlHeaderOverrides:
    @pytest.fixture(scope="class")
    def models(self):
        return {"inc_header.sql": model_sql()}

    def _write_model(self, project, **configs):
        write_file(model_sql(**configs), project.project_root, "models", "inc_header.sql")

    def test_sql_header_is_inherited_and_can_be_switched_off(self, project):
        run_dbt(["run"])

        # with no per-statement key set, `sql_header` still goes in front of every
        # statement -- the behaviour models had before per-statement headers existed
        self._write_model(project, sql_header=BAD_PRAGMA)
        results = run_dbt(["run"], expect_pass=False)
        assert "Invalid tx id: 99" in results[0].message

        # an empty per-statement key means "emit no header for this statement"
        self._write_model(
            project, sql_header=BAD_PRAGMA, merge_sql_header="", tmp_sql_header=""
        )
        run_dbt(["run"])
