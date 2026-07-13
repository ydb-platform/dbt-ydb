from dbt.tests.adapter.utils.test_current_timestamp import BaseCurrentTimestampNaive

# YDB's CurrentUtcTimestamp() returns a naive (tz-less) UTC timestamp.


class TestCurrentTimestamp(BaseCurrentTimestampNaive):
    pass
