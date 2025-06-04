import pytest

# import os
# import json

# Import the fuctional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        "type": "ydb",
        # "threads": 4,
        "host": "localhost",
        "port": 2136,
        "database": "/local",
    }
