import pytest

from dbt.tests.adapter.utils import fixture_safe_cast
from dbt.tests.adapter.utils.test_safe_cast import BaseSafeCast

from tests.functional.adapter.utils.base import YDBUtils

models__test_safe_cast_sql = """
select
    {{ safe_cast('field', api.Column.translate_type('string')) }} as actual,
    output as expected
from {{ ref('data_safe_cast') }}
"""

# The stock seed leaves the (null) `field` as the primary key, which YDB rejects.
# Prepend a surrogate `id` so it becomes the default (NOT NULL) primary key.
seeds__data_safe_cast_csv = """id,field,output
1,abc,abc
2,123,123
3,,
"""


class TestSafeCast(YDBUtils, BaseSafeCast):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_safe_cast.csv": seeds__data_safe_cast_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_safe_cast.yml": fixture_safe_cast.models__test_safe_cast_yml,
            "test_safe_cast.sql": self.interpolate_macro_namespace(
                models__test_safe_cast_sql, "safe_cast"
            ),
        }
