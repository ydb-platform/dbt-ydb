from dbt.tests.adapter.utils.test_escape_single_quotes import (
    BaseEscapeSingleQuotesBackslash,
)

from tests.functional.adapter.utils.base import YDBUtils

# YDB escapes single quotes with a backslash ('they\\'re'), not by doubling them.


class TestEscapeSingleQuotes(YDBUtils, BaseEscapeSingleQuotesBackslash):
    pass
