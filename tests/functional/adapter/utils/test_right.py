import pytest

from dbt.tests.adapter.utils import fixture_right
from dbt.tests.adapter.utils.test_right import BaseRight

from tests.functional.adapter.utils.base import YDBUtils

# Surrogate `id` primary key (string_text has duplicates).
seeds__data_right_csv = """id,string_text,length_expression,output
1,abcdef,3,def
2,fishtown,4,town
3,december,5,ember
4,december,0,
"""

models__test_right_sql = """
select
    {{ right('string_text', 'length_expression') }} as actual,
    coalesce(output, '') as expected
from {{ ref('data_right') }}
"""


class TestRight(YDBUtils, BaseRight):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_right.csv": seeds__data_right_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_right.yml": fixture_right.models__test_right_yml,
            "test_right.sql": self.interpolate_macro_namespace(
                models__test_right_sql, "right"
            ),
        }
