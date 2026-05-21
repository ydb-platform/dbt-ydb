import pytest

from dbt.tests.adapter.simple_seed import fixtures, seeds
from dbt.tests.adapter.simple_seed.test_seed_type_override import BaseSimpleSeedColumnOverride


properties__schema_yml = """
version: 2
seeds:
- name: seed_enabled
  columns:
  - name: birthday
    data_tests:
    - column_type:
        type: Text
  - name: seed_id
    data_tests:
    - column_type:
        type: Text

- name: seed_tricky
  columns:
  - name: seed_id
    data_tests:
    - column_type:
        type: Int64
  - name: seed_id_str
    data_tests:
    - column_type:
        type: Text
  - name: a_bool
    data_tests:
    - column_type:
        type: Bool
  - name: looks_like_a_bool
    data_tests:
    - column_type:
        type: Text
  - name: a_date
    data_tests:
    - column_type:
        type: Datetime
  - name: looks_like_a_date
    data_tests:
    - column_type:
        type: Text
  - name: relative
    data_tests:
    - column_type:
        type: Text
  - name: weekday
    data_tests:
    - column_type:
        type: Text
"""


class TestSimpleSeedColumnOverride(BaseSimpleSeedColumnOverride):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": properties__schema_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"schema_test.sql": fixtures.macros__schema_test}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed_enabled.csv": seeds.seeds__enabled_in_config_csv,
            "seed_disabled.csv": seeds.seeds__disabled_in_config_csv,
            "seed_tricky.csv": seeds.seeds__tricky_csv,
        }

    @staticmethod
    def seed_enabled_types():
        return {
            "seed_id": "Utf8",
            "birthday": "Utf8",
        }

    @staticmethod
    def seed_tricky_types():
        return {
            "seed_id_str": "Utf8",
            "looks_like_a_bool": "Utf8",
            "looks_like_a_date": "Utf8",
        }
