from dbt.adapters.sql import SQLAdapter
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.cache import _make_ref_key_dict
from dbt.adapters.events.types import ColTypeChange, SchemaCreation, SchemaDrop
from dbt_common.events.functions import fire_event



from dbt.adapters.ydb import YDBConnectionManager
from dbt.adapters.ydb.relation import YDBRelation

from typing import List


class YDBAdapter(SQLAdapter):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    ConnectionManager = YDBConnectionManager
    Relation = YDBRelation


    @classmethod
    def date_function(cls):
        """
        Returns canonical date func
        """
        return "CurrentUtcDate()"

    def list_relations_without_caching(
        self,
        schema_relation: BaseRelation,
    ) -> List[BaseRelation]:
        connection = self.connections.get_thread_connection()
        dbapi_connection = connection.handle
        table_names = dbapi_connection.get_table_names()


        relations = []
        for table_name in table_names:
            relations.append(
                self.Relation.create(
                    database="/local",
                    schema="",
                    identifier=table_name,
                    quote_character="`",
                    type="table",
                )
            )
        return relations

    def create_schema(self, relation: BaseRelation) -> None:
        relation = relation.without_identifier()
        fire_event(SchemaCreation(relation=_make_ref_key_dict(relation)))
        # kwargs = {
        #     "relation": relation,
        # }

        connection = self.connections.get_thread_connection()
        dbapi_connection = connection.handle

        # dbapi_connection.

        print(relation)
        print(relation)

        # self.execute_macro(CREATE_SCHEMA_MACRO_NAME, kwargs=kwargs)
        # self.commit_if_has_connection()
        # we can't update the cache here, as if the schema already existed we
        # don't want to (incorrectly) say that it's empty




# may require more build out to make more user friendly to confer with team and community.
