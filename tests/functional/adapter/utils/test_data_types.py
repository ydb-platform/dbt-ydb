from dbt.tests.adapter.utils.data_types.test_type_bigint import BaseTypeBigInt
from dbt.tests.adapter.utils.data_types.test_type_int import BaseTypeInt
from dbt.tests.adapter.utils.data_types.test_type_string import BaseTypeString

# YDB type mapping: string -> Text, int/bigint -> Int64 (the macros for
# float/numeric/boolean/timestamp exist in utils.sql for real usage).
#
# The standard tests for the remaining types can't run as-is against YDB:
#   * float   - the seed is a lone `Double` column, which YDB can't use as the
#               (mandatory) primary key.
#   * numeric - the seed casts to numeric(28,6); YDB Decimal is (22,9).
#   * boolean - casts the string 'True' to Bool, which YDB CAST rejects.
#   * timestamp - the 'YYYY-MM-DD HH:MM:SS' literal parses to NULL under YDB CAST.


class TestTypeString(BaseTypeString):
    pass


class TestTypeInt(BaseTypeInt):
    pass


class TestTypeBigInt(BaseTypeBigInt):
    pass
