import pytest

from dbt.tests.adapter.utils import fixture_hash
from dbt.tests.adapter.utils.test_hash import BaseHash

from tests.functional.adapter.utils.base import YDBUtils

models__test_hash_sql = """
select
    {{ hash('input_1') }} as actual,
    output as expected
from (
    select
        {{ replace_empty('input_1') }} as input_1,
        {{ replace_empty('output') }} as output
    from {{ ref('data_hash') }}
) as data
"""


class TestHash(YDBUtils, BaseHash):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_hash.yml": fixture_hash.models__test_hash_yml,
            "test_hash.sql": self.interpolate_macro_namespace(
                models__test_hash_sql, "hash"
            ),
        }
