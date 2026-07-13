import pytest

from dbt.tests.adapter.utils import fixture_dateadd
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd

from tests.functional.adapter.utils.base import YDBUtils

# Surrogate `id` PK (from_time repeats and the last row is NULL). Columns are
# left to agate type inference (-> DateTime), so the base timestamp column_types
# override is dropped.
seeds__data_dateadd_csv = """id,from_time,interval_length,datepart,result
1,2018-01-01 01:00:00,1,day,2018-01-02 01:00:00
2,2018-01-01 01:00:00,1,month,2018-02-01 01:00:00
3,2018-01-01 01:00:00,1,year,2019-01-01 01:00:00
4,2018-01-01 01:00:00,1,hour,2018-01-01 02:00:00
5,,1,day,
"""

models__test_dateadd_sql = """
select
    case
        when datepart = 'hour' then cast({{ dateadd('hour', 'interval_length', 'from_time') }} as {{ api.Column.translate_type('timestamp') }})
        when datepart = 'day' then cast({{ dateadd('day', 'interval_length', 'from_time') }} as {{ api.Column.translate_type('timestamp') }})
        when datepart = 'month' then cast({{ dateadd('month', 'interval_length', 'from_time') }} as {{ api.Column.translate_type('timestamp') }})
        when datepart = 'year' then cast({{ dateadd('year', 'interval_length', 'from_time') }} as {{ api.Column.translate_type('timestamp') }})
        else null
    end as actual,
    result as expected
from {{ ref('data_dateadd') }}
"""


class TestDateAdd(YDBUtils, BaseDateAdd):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_dateadd.csv": seeds__data_dateadd_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_dateadd.yml": fixture_dateadd.models__test_dateadd_yml,
            "test_dateadd.sql": self.interpolate_macro_namespace(
                models__test_dateadd_sql, "dateadd"
            ),
        }
