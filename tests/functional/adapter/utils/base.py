import pytest

from dbt.tests.adapter.utils import base_utils

# YDB (YQL) parses NOT with higher precedence than '=', so the stock
# `where not {{ equals(...) }}` binds as `(not <case...end>) = 0` and fails type
# checking (Bool expected, Int32 given). Wrapping the equals() expansion in
# parentheses fixes it. Same quirk is already handled in snapshots/strategies.sql.
macros__assert_equal_sql = """
{% test assert_equal(model, actual, expected) %}
select * from {{ model }}
where not ({{ equals(actual, expected) }})
{% endtest %}
"""


class YDBUtils:
    """Mixin: YDB-compatible helper macros shared by all utils tests."""

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "equals.sql": base_utils.macros__equals_sql,
            "test_assert_equal.sql": macros__assert_equal_sql,
            "replace_empty.sql": base_utils.macros__replace_empty_sql,
        }
