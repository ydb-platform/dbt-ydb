import pytest

from dbt.tests.adapter.basic.test_table_materialization import BaseTableMaterialization

class TestRowTableMaterialization(BaseTableMaterialization):
    @pytest.fixture(scope="class")
    def models(self):
      model_sql = """
      {{
        config(
          materialized = "table",
          primary_key = "id",
          store_type = "row",
          sort = 'first_name',
          dist = 'first_name'
        )
      }}

      select * from `{{ this.schema }}/seed`
      """

      return {"materialized.sql": model_sql}


class TestColumnTableMaterialization(BaseTableMaterialization):
    @pytest.fixture(scope="class")
    def models(self):
      model_sql = """
      {{
        config(
          materialized = "table",
          primary_key = "id",
          store_type = "column",
          sort = 'first_name',
          dist = 'first_name'
        )
      }}

      select * from `{{ this.schema }}/seed`
      """

      return {"materialized.sql": model_sql}