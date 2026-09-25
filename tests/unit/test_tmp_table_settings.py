from pathlib import Path

from jinja2 import Environment


TABLE_MACRO = (
    Path(__file__).resolve().parents[2]
    / "dbt/include/ydb/macros/materializations/models/table.sql"
)


class Relation:
    def include(self, **kwargs):
        return "`events`"


def render_create_table(config, use_tmp_settings=False):
    environment = Environment(extensions=["jinja2.ext.do"])
    template = environment.from_string(TABLE_MACRO.read_text())
    macro = template.make_module(
        {
            "model": {"name": "events", "config": config},
            "config": config,
            "ydb_get_sql_header": lambda _: None,
        }
    ).ydb__create_table_as
    return macro(False, Relation(), "select 1 as id", None, use_tmp_settings)


def test_incremental_tmp_partition_count_can_differ_from_target():
    config = {
        "primary_key": "id",
        "store_type": "column",
        "contract": {"enforced": False},
        "auto_partitioning_min_partitions_count": 16,
        "tmp_auto_partitioning_min_partitions_count": 8,
    }

    target_sql = render_create_table(config)
    tmp_sql = render_create_table(config, use_tmp_settings=True)

    assert "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16" in target_sql
    assert "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 8" in tmp_sql
    assert "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 8" not in target_sql
    assert "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16" not in tmp_sql


def test_incremental_tmp_table_inherits_unoverridden_settings():
    config = {
        "primary_key": "id",
        "store_type": "column",
        "contract": {"enforced": False},
        "auto_partitioning_min_partitions_count": 16,
        "auto_partitioning_by_size": "ENABLED",
        "tmp_auto_partitioning_min_partitions_count": 8,
    }

    tmp_sql = render_create_table(config, use_tmp_settings=True)

    assert "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 8" in tmp_sql
    assert "AUTO_PARTITIONING_BY_SIZE = ENABLED" in tmp_sql
