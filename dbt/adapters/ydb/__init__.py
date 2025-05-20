from dbt.adapters.ydb.connections import YDBConnectionManager  # noqa
from dbt.adapters.ydb.connections import YDBCredentials
from dbt.adapters.ydb.impl import YDBAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import ydb


Plugin = AdapterPlugin(
    adapter=YDBAdapter, credentials=YDBCredentials, include_path=ydb.PACKAGE_PATH
)
