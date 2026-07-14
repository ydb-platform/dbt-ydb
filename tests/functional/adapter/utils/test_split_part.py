import pytest

from dbt.tests.adapter.utils import fixture_split_part
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart

from tests.functional.adapter.utils.base import YDBUtils

# Surrogate `id` primary key (the empty `parts` row would be a NULL key).
seeds__data_split_part_csv = """id,parts,split_on,result_1,result_2,result_3,result_4
1,a|b|c,|,a,b,c,c
2,1|2|3,|,1,2,3,3
3,,|,,,,
"""

models__test_split_part_sql = """
select
    {{ split_part('parts', 'split_on', 1) }} as actual,
    result_1 as expected
from {{ ref('data_split_part') }}

union all

select
    {{ split_part('parts', 'split_on', 2) }} as actual,
    result_2 as expected
from {{ ref('data_split_part') }}

union all

select
    {{ split_part('parts', 'split_on', 3) }} as actual,
    result_3 as expected
from {{ ref('data_split_part') }}

union all

select
    {{ split_part('parts', 'split_on', -1) }} as actual,
    result_4 as expected
from {{ ref('data_split_part') }}
"""


class TestSplitPart(YDBUtils, BaseSplitPart):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_split_part.csv": seeds__data_split_part_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_split_part.yml": fixture_split_part.models__test_split_part_yml,
            "test_split_part.sql": self.interpolate_macro_namespace(
                models__test_split_part_sql, "split_part"
            ),
        }
