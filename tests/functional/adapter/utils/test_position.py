import pytest

from dbt.tests.adapter.utils import fixture_position
from dbt.tests.adapter.utils.test_position import BasePosition

from tests.functional.adapter.utils.base import YDBUtils

models__test_position_sql = """
select
    {{ position('substring_text', 'string_text') }} as actual,
    result as expected
from {{ ref('data_position') }}
"""


class TestPosition(YDBUtils, BasePosition):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_position.yml": fixture_position.models__test_position_yml,
            "test_position.sql": self.interpolate_macro_namespace(
                models__test_position_sql, "position"
            ),
        }
