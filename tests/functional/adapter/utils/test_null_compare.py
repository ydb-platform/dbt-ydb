from dbt.tests.adapter.utils.test_null_compare import (
    BaseMixedNullCompare,
    BaseNullCompare,
)

from tests.functional.adapter.utils.base import YDBUtils


class TestMixedNullCompare(YDBUtils, BaseMixedNullCompare):
    pass


class TestNullCompare(YDBUtils, BaseNullCompare):
    pass
