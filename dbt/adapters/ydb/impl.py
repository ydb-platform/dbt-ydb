from dbt.adapters.sql import SQLAdapter
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.base.impl import ConstraintSupport, ConstraintType
from dbt.adapters.cache import _make_ref_key_dict
from dbt.adapters.events.types import SchemaCreation, SchemaDrop
import dbt_common.clients
from dbt_common.exceptions import DbtRuntimeError, CompilationError
from dbt_common.events.functions import fire_event
from dbt.adapters.base.relation import InformationSchema

from dbt.adapters.base.meta import available
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.clients.agate_helper import DEFAULT_TYPE_TESTER
# from dbt.adapters.base.column import Column

from dbt.adapters.ydb import YDBConnectionManager
from dbt.adapters.ydb.column import YDBColumn
from dbt.adapters.ydb.relation import YDBRelation

from typing import Any, Dict, FrozenSet, List, TYPE_CHECKING, Optional, Set, Tuple


if TYPE_CHECKING:
    import agate

logger = AdapterLogger("YDB")

class YDBAdapter(SQLAdapter):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    ConnectionManager = YDBConnectionManager
    Column = YDBColumn
    Relation = YDBRelation

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.unique: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.primary_key: ConstraintSupport.ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_SUPPORTED,
    }

    @classmethod
    def quote(self, identifier):
        return '`{}`'.format(identifier)

    @classmethod
    def date_function(cls):
        """
        Returns canonical date func
        """
        return "CurrentUtcDate()"

    @classmethod
    def convert_text_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return 'Utf8'

    @classmethod
    def convert_number_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        import agate

        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return 'Double' if decimals else 'Int64'

    @classmethod
    def convert_datetime_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return 'DateTime'

    @classmethod
    def convert_date_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return 'Date'

    def list_relations_without_caching(
        self,
        schema_relation: BaseRelation,
    ) -> List[BaseRelation]:
        if not self.check_schema_exists(schema_relation.database, schema_relation.schema):
            logger.debug(f"Unable to list relations in schema {schema_relation.schema}: schema does not exist")
            return []

        connection = self.connections.get_thread_connection()
        dbapi_connection = connection.handle

        current_prefix = dbapi_connection.table_path_prefix
        dbapi_connection.table_path_prefix = schema_relation.schema

        relations = []

        for relation_type, relation_list in [
            ("table", dbapi_connection.get_table_names()),
            ("view", dbapi_connection.get_view_names()),
        ]:
            for relation in relation_list:
                r = self.Relation.create(
                        database=schema_relation.database,
                        schema=schema_relation.schema,
                        identifier=relation,
                        type=relation_type,
                    )

                relations.append(r)

        dbapi_connection.table_path_prefix = current_prefix

        return relations

    def get_columns_in_relation(self, relation):
        connection = self.connections.get_thread_connection()
        dbapi_connection = connection.handle

        logger.debug(f"Try to get columns from  {relation}")

        rel_str = relation.render().replace('`', '')

        if not dbapi_connection.check_exists(rel_str):
            logger.debug(f"Relation {rel_str} does not exist, unable to get columns")
            return []

        with dbapi_connection.cursor() as cur:
            cur.execute(f"SELECT * FROM {relation} LIMIT 1")
            return [YDBColumn(col[0], col[1]) for col in cur.description]

    def get_rows_different_sql(
        self,
        relation_a: YDBRelation,
        relation_b: YDBRelation,
        column_names: Optional[List[str]] = None,
        except_operator: Optional[str] = None,
    ) -> str:
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))

        alias_a = 'ta'
        alias_b = 'tb'
        columns_csv_a = ', '.join([f'{alias_a}.{name}' for name in names])
        columns_csv_b = ', '.join([f'{alias_b}.{name}' for name in names])
        join_condition = ' AND '.join([f'{alias_a}.{name} = {alias_b}.{name}' for name in names])
        first_column = names[0]

        sql = COLUMNS_EQUAL_SQL.format(
            alias_a=alias_a,
            alias_b=alias_b,
            first_column=first_column,
            columns_a=columns_csv_a,
            columns_b=columns_csv_b,
            join_condition=join_condition,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
        )

        return sql

    def check_schema_exists(self, database: str, schema: str) -> bool:
        connection = self.connections.get_thread_connection()
        dbapi_connection = connection.handle

        return dbapi_connection.check_exists(schema)

    def create_schema(self, relation: BaseRelation) -> None:
        relation = relation.without_identifier()
        fire_event(SchemaCreation(relation=_make_ref_key_dict(relation)))

        connection = self.connections.get_thread_connection()
        dbapi_connection = connection.handle

        if not relation.schema:
            return

        directory = f"{relation.database}/{relation.schema}"

        dbapi_connection._driver.scheme_client.make_directory(directory)

    def drop_schema(self, relation):
        if not self.check_schema_exists(relation.database, relation.schema):
            logger.debug("Unable to drop schema: does not exist")
            return
        relation = relation.without_identifier()
        fire_event(SchemaDrop(relation=_make_ref_key_dict(relation)))

        for inner_relation in self.list_relations_without_caching(relation):
            logger.debug(f"Drop child relation before drop schema: {inner_relation}")
            self.drop_relation(inner_relation)

        connection = self.connections.get_thread_connection()
        dbapi_connection = connection.handle

        if not relation.schema:
            return

        directory = f"{relation.database}/{relation.schema}"
        dbapi_connection._driver.scheme_client.remove_directory(directory)

        # we can update the cache here
        self.cache.drop_schema(relation.database, relation.schema)

    @available
    def prepare_insert_values_from_csv(self, table):
        import agate
        csv_funcs = [c.csvify for c in table._column_types]

        out = []
        def prepare_value(i, val):
            ctype = table._column_types[i]
            if isinstance(ctype, agate.data_types.DateTime):
                return f'DateTime("{val}Z")'
            if isinstance(ctype, agate.data_types.Date):
                return f'Date("{val}")'
            if isinstance(ctype, dbt_common.clients.agate_helper.Number):
                return f"{val}"
            return f'"{val}"'

        for row in table.rows:
            out.append(f"({', '.join(prepare_value(i, csv_funcs[i](d)) for i, d in enumerate(row))})")

        return ",".join(out)

    def rename_relation(self, from_relation: YDBRelation, to_relation: YDBRelation):
        logger.debug(f"Try to rename relation from {from_relation}[{str(from_relation.type)}] to {to_relation}[{str(to_relation.type)}]")
        self.drop_relation(to_relation)
        if from_relation.type == "table":
            import ydb
            connection = self.connections.get_thread_connection()
            dbapi_connection = connection.handle

            from_path = f"{from_relation.database}/{from_relation.render()[1:-1]}".strip()
            to_path = f"{to_relation.database}/{to_relation.render()[1:-1]}".strip()

            rename_item = ydb.RenameItem(
                from_path,
                to_path,
            )

            dbapi_connection._driver.table_client.rename_tables([rename_item])
        else:
            raise DbtRuntimeError("View rename does not supported in YDB")
        self.cache_renamed(from_relation, to_relation)

    def get_catalog_for_single_relation(self, relation):
        columns: List[Dict[str, Any]] = []
        logger.debug("Getting table schema for relation {}", str(relation))
        columns.extend(self.parse_columns_from_relation(relation))

        import agate

        return agate.Table.from_object(columns, column_types=DEFAULT_TYPE_TESTER)

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Set[str],
        used_schemas: FrozenSet[Tuple[str, str]],
    ) -> "agate.Table":
        if len(schemas) != 1:
            raise CompilationError(
                f"Expected only one schema in ydb _get_one_catalog, found " f"{schemas}"
            )

        database = information_schema.database
        schema = list(schemas)[0]

        columns: List[Dict[str, Any]] = []
        relations = self.list_relations(database, schema)
        for relation in relations:
            logger.debug("Getting table schema for relation {}", str(relation))
            columns.extend(self.parse_columns_from_relation(relation))

        import agate

        return agate.Table.from_object(columns, column_types=DEFAULT_TYPE_TESTER)

    def parse_columns_from_relation(self, relation: BaseRelation) -> List[Dict[str, Any]]:
        owner = ""
        ydb_columns = self.get_columns_in_relation(relation)
        columns = []
        for col_num, col in enumerate(ydb_columns):
            column = {
                "table_database": relation.database,
                "table_schema": relation.schema,
                "table_name": relation.identifier,
                "table_type": relation.type,
                "column_index": col_num,
                "table_owner": owner,
                "column_name": col.name,
                "column_type": col.dtype
            }
            columns.append(column)
        return columns

    def valid_incremental_strategies(self):
        """The set of standard builtin strategies which this adapter supports out-of-the-box.
        Not used to validate custom strategies defined by end users.
        """
        return ["merge"]

    @available.parse_none
    def get_column_schema_from_query(self, sql: str, *_) -> List[YDBColumn]:
        logger.info(f"Try to get column schema from query: \n{sql}")
        connection = self.connections.get_thread_connection()
        dbapi_connection = connection.handle

        with dbapi_connection.cursor() as cur:
            cur.execute(sql)
            return [YDBColumn(col[0], col[1]) for col in cur.description]



COLUMNS_EQUAL_SQL = '''
$rel_a = SELECT COUNT(*) as num_rows FROM {relation_a};
$rel_b = SELECT COUNT(*) as num_rows FROM {relation_b};
SELECT
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
FROM (
    SELECT
        1 as id,
        $rel_a - $rel_b as difference
    ) as row_count_diff
INNER JOIN (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM (
            SELECT
                {columns_a}
            FROM {relation_a} as {alias_a}
            LEFT OUTER JOIN {relation_b} as {alias_b}
                ON {join_condition}
            WHERE {alias_b}.{first_column} IS NULL
            UNION ALL
            SELECT
                {columns_b}
            FROM {relation_b} as {alias_b}
            LEFT OUTER JOIN {relation_a} as {alias_a}
                ON {join_condition}
            WHERE {alias_a}.{first_column} IS NULL
        ) as missing
    ) as diff_count ON row_count_diff.id = diff_count.id
'''.strip()