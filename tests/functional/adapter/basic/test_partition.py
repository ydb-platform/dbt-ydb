import pytest

from dbt.tests.util import relation_from_name, run_dbt

seeds__data_csv = """id,name
1,alice
2,bob
3,carol
"""

# Column-oriented table with an explicit PARTITION BY HASH clause.
column_partition_sql = """
{{ config(
    materialized='table',
    store_type='column',
    primary_key='id',
    partition_by='id',
    partition_method='hash',
    auto_partitioning_min_partitions_count=4
) }}
select * from {{ ref('data') }}
"""

# Row-oriented table with WITH(...) partitioning settings.
row_partition_sql = """
{{ config(
    materialized='table',
    store_type='row',
    primary_key='id',
    auto_partitioning_by_load='ENABLED',
    auto_partitioning_min_partitions_count=2,
    auto_partitioning_max_partitions_count=8
) }}
select * from {{ ref('data') }}
"""

# PARTITION BY HASH is column-only -> should raise at compile time on a row table.
bad_partition_sql = """
{{ config(
    materialized='table',
    store_type='row',
    primary_key='id',
    partition_by='id'
) }}
select * from {{ ref('data') }}
"""


class BasePartition:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data.csv": seeds__data_csv}

    def _assert_rows(self, project, name, expected=3):
        relation = relation_from_name(project.adapter, name)
        result = project.run_sql(f"select count(*) as n from {relation}", fetch="one")
        assert result[0] == expected


class TestColumnPartitionBy(BasePartition):
    @pytest.fixture(scope="class")
    def models(self):
        return {"col_partitioned.sql": column_partition_sql}

    def test_partition_by(self, project):
        run_dbt(["build"])
        self._assert_rows(project, "col_partitioned")


class TestRowPartitionSettings(BasePartition):
    @pytest.fixture(scope="class")
    def models(self):
        return {"row_partitioned.sql": row_partition_sql}

    def test_partition_settings(self, project):
        run_dbt(["build"])
        self._assert_rows(project, "row_partitioned")


class TestPartitionByRequiresColumnStore(BasePartition):
    @pytest.fixture(scope="class")
    def models(self):
        return {"bad_partitioned.sql": bad_partition_sql}

    def test_compile_error(self, project):
        run_dbt(["seed"])
        run_dbt(["run"], expect_pass=False)
