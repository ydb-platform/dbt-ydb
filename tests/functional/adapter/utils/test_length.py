import pytest

from dbt.tests.adapter.utils import fixture_length
from dbt.tests.adapter.utils.test_length import BaseLength

from tests.functional.adapter.utils.base import YDBUtils

# YDB has no CTE support -> inline the `with data as (...)` subquery.
models__test_length_sql = """
select
    {{ length('expression') }} as actual,
    output as expected
from {{ ref('data_length') }}
"""


class TestLength(YDBUtils, BaseLength):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_length.yml": fixture_length.models__test_length_yml,
            "test_length.sql": self.interpolate_macro_namespace(
                models__test_length_sql, "length"
            ),
        }
