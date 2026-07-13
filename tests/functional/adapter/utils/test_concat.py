import pytest

from dbt.tests.adapter.utils import fixture_concat
from dbt.tests.adapter.utils.test_concat import BaseConcat

from tests.functional.adapter.utils.base import YDBUtils

# Surrogate `id` primary key (input_1 has duplicates).
seeds__data_concat_csv = """id,input_1,input_2,output
1,a,b,ab
2,a,EMPTY,a
3,EMPTY,b,b
4,EMPTY,EMPTY,EMPTY
"""

models__test_concat_sql = """
select
    {{ concat(['input_1', 'input_2']) }} as actual,
    output as expected
from (
    select
        {{ replace_empty('input_1') }} as input_1,
        {{ replace_empty('input_2') }} as input_2,
        {{ replace_empty('output') }} as output
    from {{ ref('data_concat') }}
) as data
"""


class TestConcat(YDBUtils, BaseConcat):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_concat.csv": seeds__data_concat_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_concat.yml": fixture_concat.models__test_concat_yml,
            "test_concat.sql": self.interpolate_macro_namespace(
                models__test_concat_sql, "concat"
            ),
        }
