import pytest

from dbt.tests.adapter.utils import fixture_date_trunc
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc

from tests.functional.adapter.utils.base import YDBUtils

# Surrogate `id` PK (updated_at is NULL on the second row). `day`/`month` are
# quoted since they collide with YQL keywords.
seeds__data_date_trunc_csv = """id,updated_at,day,month
1,2018-01-05 12:00:00,2018-01-05,2018-01-01
2,,,
"""

models__test_date_trunc_sql = """
select
    cast({{ date_trunc('day', 'updated_at') }} as date) as actual,
    `day` as expected
from {{ ref('data_date_trunc') }}

union all

select
    cast({{ date_trunc('month', 'updated_at') }} as date) as actual,
    `month` as expected
from {{ ref('data_date_trunc') }}
"""


class TestDateTrunc(YDBUtils, BaseDateTrunc):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_date_trunc.csv": seeds__data_date_trunc_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_trunc.yml": fixture_date_trunc.models__test_date_trunc_yml,
            "test_date_trunc.sql": self.interpolate_macro_namespace(
                models__test_date_trunc_sql, "date_trunc"
            ),
        }
