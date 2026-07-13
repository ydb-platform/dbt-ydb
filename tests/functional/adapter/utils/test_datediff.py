import pytest

from dbt.tests.adapter.utils import fixture_datediff
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff

from tests.functional.adapter.utils.base import YDBUtils

# The stock model also unions in literal cases like
#   datediff("'1999-12-31 23:59:59.999999'", ...)
# but YDB's CAST returns NULL for the `YYYY-MM-DD HH:MM:SS.ffffff` literal format,
# so those rows can't be represented. We keep the full seed-driven matrix (every
# datepart incl. the tricky week boundaries and NULL rows) and add a `quarter`
# case, which is the realistic column-based usage the ydb__datediff macro targets.
seeds__data_datediff_csv = """id,first_date,second_date,datepart,result
1,2018-01-01 01:00:00,2018-01-02 01:00:00,day,1
2,2018-01-01 01:00:00,2018-02-01 01:00:00,month,1
3,2018-01-01 01:00:00,2019-01-01 01:00:00,year,1
4,2018-01-01 01:00:00,2018-01-01 02:00:00,hour,1
5,2018-01-01 01:00:00,2018-01-01 02:01:00,minute,61
6,2018-01-01 01:00:00,2018-01-01 02:00:01,second,3601
7,2019-12-31 00:00:00,2019-12-27 00:00:00,week,-1
8,2019-12-31 00:00:00,2019-12-30 00:00:00,week,0
9,2019-12-31 00:00:00,2020-01-02 00:00:00,week,0
10,2019-12-31 00:00:00,2020-01-06 02:00:00,week,1
11,2018-01-01 00:00:00,2018-07-01 00:00:00,quarter,2
12,,2018-01-01 02:00:00,hour,
13,2018-01-01 02:00:00,,hour,
"""

models__test_datediff_sql = """
select
    case
        when datepart = 'second' then {{ datediff('first_date', 'second_date', 'second') }}
        when datepart = 'minute' then {{ datediff('first_date', 'second_date', 'minute') }}
        when datepart = 'hour' then {{ datediff('first_date', 'second_date', 'hour') }}
        when datepart = 'day' then {{ datediff('first_date', 'second_date', 'day') }}
        when datepart = 'week' then {{ datediff('first_date', 'second_date', 'week') }}
        when datepart = 'month' then {{ datediff('first_date', 'second_date', 'month') }}
        when datepart = 'quarter' then {{ datediff('first_date', 'second_date', 'quarter') }}
        when datepart = 'year' then {{ datediff('first_date', 'second_date', 'year') }}
        else null
    end as actual,
    result as expected
from {{ ref('data_datediff') }}
"""


class TestDateDiff(YDBUtils, BaseDateDiff):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_datediff.csv": seeds__data_datediff_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_datediff.yml": fixture_datediff.models__test_datediff_yml,
            "test_datediff.sql": self.interpolate_macro_namespace(
                models__test_datediff_sql, "datediff"
            ),
        }
