import pytest

from dbt.tests.adapter.simple_seed.test_seed import (
    BaseSeedSpecificFormats,
    BaseSimpleSeedEnabledViaConfig,
    BaseTestEmptySeed,
)


class TestSimpleSeedEnabledViaConfig(BaseSimpleSeedEnabledViaConfig):
    @pytest.fixture(scope="function")
    def clear_test_schema(self, project):
        project.drop_test_schema()
        project.create_test_schema()
        yield
        project.drop_test_schema()
        project.create_test_schema()


class TestSeedSpecificFormats(BaseSeedSpecificFormats):
    pass


class TestEmptySeed(BaseTestEmptySeed):
    pass
