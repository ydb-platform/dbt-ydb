import pytest

from dbt.tests.adapter.basic import files
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests

config_materialized_table = """
  {{ config(materialized="table", primary_key="id") }}
"""

model_base = """
  select * from {{ source('raw', 'seed') }}
"""

class TestGenericTests(BaseGenericTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": files.base_view_sql,
            "table_model.sql": config_materialized_table + model_base,
            "schema.yml": files.schema_base_yml,
            "schema_view.yml": files.generic_test_view_yml,
            "schema_table.yml": files.generic_test_table_yml,
        }
