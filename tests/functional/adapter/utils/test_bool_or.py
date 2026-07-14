import pytest

from dbt.tests.adapter.utils import fixture_bool_or
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr

from tests.functional.adapter.utils.base import YDBUtils

# Surrogate `id` primary key (key_column repeats by design).
seeds__data_bool_or_csv = """id,key_column,val1,val2
1,abc,1,1
2,abc,1,0
3,def,1,0
4,hij,1,1
5,hij,1,
6,klm,1,0
7,klm,1,
"""

models__test_bool_or_sql = """
select
    calculate.value as actual,
    data_output.value as expected
from (
    select
        key_column,
        {{ bool_or('val1 = val2') }} as value
    from {{ ref('data_bool_or') }}
    group by key_column
) as calculate
left join {{ ref('data_bool_or_expected') }} as data_output
    on calculate.key_column = data_output.key_column
"""


class TestBoolOr(YDBUtils, BaseBoolOr):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_bool_or.csv": seeds__data_bool_or_csv,
            "data_bool_or_expected.csv": fixture_bool_or.seeds__data_bool_or_expected_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_bool_or.yml": fixture_bool_or.models__test_bool_or_yml,
            "test_bool_or.sql": self.interpolate_macro_namespace(
                models__test_bool_or_sql, "bool_or"
            ),
        }
