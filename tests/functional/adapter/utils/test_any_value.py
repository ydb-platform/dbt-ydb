import pytest

from dbt.tests.adapter.utils import fixture_any_value
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue

from tests.functional.adapter.utils.base import YDBUtils

models__test_any_value_sql = """
select
    calculate.num_rows as actual,
    data_output.num_rows as expected
from (
    select
        key_name,
        {{ any_value('static_col') }} as static_col,
        count(id) as num_rows
    from {{ ref('data_any_value') }}
    group by key_name
) as calculate
left join {{ ref('data_any_value_expected') }} as data_output
    on calculate.key_name = data_output.key_name
    and calculate.static_col = data_output.static_col
"""


class TestAnyValue(YDBUtils, BaseAnyValue):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_any_value.yml": fixture_any_value.models__test_any_value_yml,
            "test_any_value.sql": self.interpolate_macro_namespace(
                models__test_any_value_sql, "any_value"
            ),
        }
