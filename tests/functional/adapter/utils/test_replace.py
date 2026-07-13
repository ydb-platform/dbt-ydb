import pytest

from dbt.tests.adapter.utils import fixture_replace
from dbt.tests.adapter.utils.test_replace import BaseReplace

from tests.functional.adapter.utils.base import YDBUtils

# YQL forbids a bare `*` alongside other projection items -> qualify with alias.
models__test_replace_sql = """
select
    {{ replace('string_text', 'old_chars', 'new_chars') }} as actual,
    result as expected
from (
    select
        src.*,
        coalesce(search_chars, '') as old_chars,
        coalesce(replace_chars, '') as new_chars
    from {{ ref('data_replace') }} as src
) as data
"""


class TestReplace(YDBUtils, BaseReplace):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_replace.yml": fixture_replace.models__test_replace_yml,
            "test_replace.sql": self.interpolate_macro_namespace(
                models__test_replace_sql, "replace"
            ),
        }
