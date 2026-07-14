import pytest

from dbt.tests.adapter.utils import fixture_equals
from dbt.tests.adapter.utils.test_equals import BaseEquals

from tests.functional.adapter.utils.base import YDBUtils

# No CTE; and `not equals(...)` must be parenthesised (YDB binds NOT tighter than =).
models__equal_values_sql = """
select *
from {{ ref('data_equals') }}
where {{ equals('x', 'y') }}
"""

models__not_equal_values_sql = """
select *
from {{ ref('data_equals') }}
where not ({{ equals('x', 'y') }})
"""


class TestEquals(YDBUtils, BaseEquals):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_equals.csv": fixture_equals.SEEDS__DATA_EQUALS_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "equal_values.sql": models__equal_values_sql,
            "not_equal_values.sql": models__not_equal_values_sql,
        }
