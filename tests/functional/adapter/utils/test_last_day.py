import pytest

from dbt.tests.adapter.utils import fixture_last_day
from dbt.tests.adapter.utils.test_last_day import BaseLastDay

from tests.functional.adapter.utils.base import YDBUtils

# Surrogate `id` PK (date_day repeats and the last row is NULL).
seeds__data_last_day_csv = """id,date_day,date_part,result
1,2018-01-02,month,2018-01-31
2,2018-01-02,quarter,2018-03-31
3,2018-01-02,year,2018-12-31
4,,month,
"""

models__test_last_day_sql = """
select
    case
        when date_part = 'month' then {{ last_day('date_day', 'month') }}
        when date_part = 'quarter' then {{ last_day('date_day', 'quarter') }}
        when date_part = 'year' then {{ last_day('date_day', 'year') }}
        else null
    end as actual,
    result as expected
from {{ ref('data_last_day') }}
"""


class TestLastDay(YDBUtils, BaseLastDay):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_last_day.csv": seeds__data_last_day_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_last_day.yml": fixture_last_day.models__test_last_day_yml,
            "test_last_day.sql": self.interpolate_macro_namespace(
                models__test_last_day_sql, "last_day"
            ),
        }
