from dbt.tests.adapter.utils.test_intersect import BaseIntersect

# YQL supports INTERSECT natively; the set-operation materialises as a view.


class TestIntersect(BaseIntersect):
    pass
