import pytest

from dbt.tests.adapter.basic import files
from dbt.tests.adapter.basic.test_incremental import BaseIncremental, BaseIncrementalNotSchemaChange


config_materialized_incremental = """
  {{ config(materialized="incremental", primary_key="id") }}
"""

model_incremental = """
select * from {{ source('raw', 'seed') }}
{% if is_incremental() %}
where id > 10
{% endif %}
""".strip()

incremental_not_schema_change_sql = """
{{ config(
    materialized="incremental",
    primary_key="user_id_current_time",
    unique_key="user_id_current_time",
    on_schema_change="sync_all_columns"
) }}
select
    '1' || '-' || Cast(CurrentUtcTimestamp() as Text) as user_id_current_time,
    {% if is_incremental() %}
        'thisis18characters' as platform
    {% else %}
        'okthisis20characters' as platform
    {% endif %}
"""

class TestIncremental(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental.sql": config_materialized_incremental + model_incremental,
            "schema.yml": files.schema_base_yml,
        }



class TestIncrementalNotSchemaChange(BaseIncrementalNotSchemaChange):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_not_schema_change.sql": incremental_not_schema_change_sql}
