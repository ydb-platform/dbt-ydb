from contextlib import contextmanager
from dataclasses import dataclass
from dbt_common.exceptions import DbtDatabaseError
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.contracts.connection import Connection
import time
from dbt.adapters.sql import SQLConnectionManager

from dbt.adapters.events.logging import AdapterLogger

import ydb_dbapi as dbapi

from typing import Any, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    import agate


logger = AdapterLogger("YDB")


@dataclass
class YDBCredentials(Credentials):
    """
    Defines database specific credentials that get added to
    profiles.yml to connect to new adapter
    """

    host: str = "localhost"
    port: int = 2136
    database: str = "/local"
    schema: str = ""
    secure: bool = False

    username: Optional[str] = None
    password: Optional[str] = None

    token: Optional[str] = None

    service_account_credentials_file: Optional[str] = None

    root_certificates_path: Optional[str] = None

    _ALIASES = {"dbname": "database", "pass": "password", "user": "username"}

    @property
    def type(self):
        """Return name of adapter."""
        return "ydb"

    @property
    def unique_field(self):
        """
        Hashed and included in anonymous telemetry to track adapter adoption.
        Pick a field that can uniquely identify one team/organization building with this adapter
        """
        return self.host

    def _connection_keys(self):
        """
        List of keys to display in the `dbt debug` output.
        """
        return ("host", "port", "username", "user", "database", "schema")

    def _get_ydb_credentials(self):
        import ydb

        if self.username is not None:
            return ydb.StaticCredentials.from_user_password(
                self.username, self.password
            )

        if self.token is not None:
            return ydb.AccessTokenCredentials(self.token)

        if self.service_account_credentials_file is not None:
            import ydb.iam

            return ydb.iam.ServiceAccountCredentials.from_file(
                self.service_account_credentials_file
            )

        return ydb.AnonymousCredentials()


class YDBConnectionManager(SQLConnectionManager):
    TYPE = "ydb"

    @contextmanager
    def exception_handler(self, sql: str):
        """
        Returns a context manager, that will handle exceptions raised
        from queries, catch, log, and raise dbt exceptions it knows how to handle.
        """
        ## Example ##
        try:
            yield
        except dbapi.DatabaseError as exc:
            self.release()

            logger.debug("YDB error: {}".format(str(exc)))
            raise DbtDatabaseError(str(exc))
        except Exception as exc:
            logger.debug("Error running SQL: {}".format(sql))
            logger.debug("Rolling back transaction.")
            self.release()
            raise DbtDatabaseError(str(exc))

    @classmethod
    def open(cls, connection):
        """
        Receives a connection object and a Credentials object
        and moves it to the "open" state.
        """
        ## Example ##
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials

        try:
            handle = dbapi.connect(
                host=credentials.host,
                port=credentials.port,
                database=credentials.database,
                protocol="grpcs" if credentials.secure else "grpc",
                credentials=credentials._get_ydb_credentials(),
                root_certificates_path=credentials.root_certificates_path,
            )
            logger.debug("Connect success")
            connection.state = "open"
            connection.handle = handle
        except dbapi.Error as exc:
            logger.debug("YDB error: {}".format(str(exc)))
            raise DbtDatabaseError(str(exc))

        return connection

    @classmethod
    def get_response(cls, _):
        return "OK"

    def cancel(self, connection):
        """
        Gets a connection object and attempts to cancel any ongoing queries.
        """
        connection_name = connection.name
        logger.debug("Cancelling query '{}'", connection_name)
        connection.handle.close()
        logger.debug("Cancel query '{}'", connection_name)

    def begin(self):
        pass

    def commit(self):
        pass

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, "agate.Table"]:
        from dbt_common.clients.agate_helper import empty_table

        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        if fetch:
            table = self.get_result_from_cursor(cursor, limit)
        else:
            table = empty_table()
        return AdapterResponse(response), table

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
    ) -> Tuple[Connection, Any]:
        sql = self._add_query_comment(sql)
        conn = self.get_thread_connection()
        dbapi_connection = conn.handle
        with self.exception_handler(sql):
            logger.debug(f"On {conn.name}: {sql}...")
            pre = time.time()
            cursor = dbapi_connection.cursor()
            logger.debug(f"Try to execute SQL: \n{sql}")
            cursor.execute_scheme(sql) # TODO: split
            status = self.get_response(cursor)
            logger.debug(
                f"SQL status: {status} in {(time.time() - pre):0.2f} seconds"
            )
            return conn, cursor
