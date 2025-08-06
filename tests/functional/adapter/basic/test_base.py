import pytest

from dbt.tests.adapter.basic import files
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations

config_materialized_table = """
  {{ config(materialized="table", primary_key="id") }}
"""

config_materialized_var = """
  {{ config(materialized=var("materialized_var", "table"), primary_key="id")}}
"""

model_base = """
  select * from {{ source('raw', 'seed') }}
"""

class TestBaseSimpleMaterializations(BaseSimpleMaterializations):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": files.base_view_sql,
            "table_model.sql": config_materialized_table + model_base,
            "swappable.sql": config_materialized_var + model_base,
            "schema.yml": files.schema_base_yml,
        }