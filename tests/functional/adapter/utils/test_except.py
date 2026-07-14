from dbt.tests.adapter.utils.test_except import BaseExcept

# YQL supports EXCEPT natively and the set-operation now materialises fine as a
# view (see the fix in materializations/models/view.sql), so no override needed.


class TestExcept(BaseExcept):
    pass
