import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod


class TestSimpleMaterializationsYDB(BaseSimpleMaterializations):
    pass


class TestSingularTestsYDB(BaseSingularTests):
    pass


class TestSingularTestsEphemeralYDB(BaseSingularTestsEphemeral):
    pass


class TestEmptyYDB(BaseEmpty):
    pass


class TestEphemeralYDB(BaseEphemeral):
    pass


class TestIncrementalYDB(BaseIncremental):
    pass


class TestGenericTestsYDB(BaseGenericTests):
    pass


class TestSnapshotCheckColsYDB(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampYDB(BaseSnapshotTimestamp):
    pass


class TestBaseAdapterMethodYDB(BaseAdapterMethod):
    pass
