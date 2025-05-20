import pytest

from dbt.tests.adapter.basic.test_table_materialization import BaseTableMaterialization

# We have to override this because of seed table render
model_sql = """
{{
  config(
    materialized = "table",
    sort = 'first_name',
    dist = 'first_name'
  )
}}

select * from `{{ this.schema }}/seed`
"""


class TestTableMaterialization(BaseTableMaterialization):
    @pytest.fixture(scope="class")
    def models(self):
        return {"materialized.sql": model_sql}
