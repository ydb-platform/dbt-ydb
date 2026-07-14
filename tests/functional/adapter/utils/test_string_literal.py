from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral

from tests.functional.adapter.utils.base import YDBUtils

# The model is plain UNION ALL (no CTE) and the default string_literal macro
# ('...') is valid YQL, so only the assert_equal NOT-precedence fix is needed.


class TestStringLiteral(YDBUtils, BaseStringLiteral):
    pass
