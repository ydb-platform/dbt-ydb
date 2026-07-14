import os

import pytest

# import json

# Import the fuctional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml.
# Host/port default to the local YDB but can be overridden via env vars (handy
# when running against a container on non-default ports).
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        "type": "ydb",
        # "threads": 4,
        "host": os.environ.get("YDB_TEST_HOST", "localhost"),
        "port": int(os.environ.get("YDB_TEST_PORT", "2136")),
        "database": os.environ.get("YDB_TEST_DATABASE", "/local"),
    }
