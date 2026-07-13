import pytest

from dbt.tests.adapter.utils import fixture_cast_bool_to_text
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText

from tests.functional.adapter.utils.base import YDBUtils

models__test_cast_bool_to_text_sql = """
select
    {{ cast_bool_to_text('input') }} as actual,
    expected
from (
    select cast(0=1 as bool) as input, 'false' as expected union all
    select cast(1=1 as bool) as input, 'true' as expected union all
    select cast(null as bool) as input, cast(null as utf8) as expected
) as data
"""


class TestCastBoolToText(YDBUtils, BaseCastBoolToText):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_cast_bool_to_text.yml": fixture_cast_bool_to_text.models__test_cast_bool_to_text_yml,
            "test_cast_bool_to_text.sql": self.interpolate_macro_namespace(
                models__test_cast_bool_to_text_sql, "cast_bool_to_text"
            ),
        }
