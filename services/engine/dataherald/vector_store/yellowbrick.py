import atexit
import logging
import os
from typing import TYPE_CHECKING, Any, List, Tuple

from langchain_community.docstore.document import Document
from langchain_community.vectorstores.yellowbrick import Yellowbrick as YellowbrickLC
from langchain_openai import OpenAIEmbeddings
from overrides import override
from sql_metadata import Parser

from dataherald.config import System
from dataherald.db import DB
from dataherald.repositories.database_connections import DatabaseConnectionRepository
from dataherald.types import GoldenSQL
from dataherald.vector_store import VectorStore

if TYPE_CHECKING:
    from psycopg2.extensions import connection as pg_connection

EMBEDDING_MODEL = "text-embedding-3-small"


class YellowbrickDataherald(YellowbrickLC):
    """Yellowbrick as a vector database for Dataherald.
    Example:
        .. code-block:: python
            from langchain_community.vectorstores import Yellowbrick
            from langchain_community.embeddings.openai import OpenAIEmbeddings
            ...
    """

    def __init__(
        self,
        embedding,
        connection_string: str,
        table: str,
        *,
        schema: str | None = None,
        logger: logging.Logger | None = None,
        drop: bool = False,
    ) -> None:
        super().__init__(
            embedding, connection_string, table, schema=schema, logger=logger, drop=drop
        )
        self.connection = YellowbrickDataherald.DatabaseConnection(
            connection_string, self.logger
        )
        atexit.register(self.connection.close_connection)

    class DatabaseConnection(YellowbrickLC.DatabaseConnection):
        _instance = None

        def __new__(cls, connection_string: str, logger: logging.Logger):
            if cls._instance is None:
                cls._instance = super().__new__(cls, connection_string, logger)
            return cls._instance

        @override
        def get_connection(self) -> "pg_connection":
            import psycopg2
            import psycopg2.extras

            try:
                if not self._connection or self._connection.closed:
                    self._connection = psycopg2.connect(self._connection_string)
                    self._connection.autocommit = False
                else:
                    cursor = self._connection.cursor()
                    cursor.execute("SELECT 1")
                    cursor.close()
            except (Exception, psycopg2.DatabaseError) as error:
                print(f"Error detected, reconnecting: {error}")
                self._connection = psycopg2.connect(self._connection_string)
                self._connection.autocommit = False

            return self._connection

    # @override -- commented out to avoid ruff rule conflict with parent function
    def delete(
        self,
        ids: List[str] | None = None,
        delete_all: bool | None = None,
        **kwargs: Any,
    ) -> None:
        """Delete vectors by uuids.

        Args:
            ids: List of ids to delete, where each id is a uuid string.
        """
        from psycopg2 import sql

        filter_expr = kwargs.get("filter_expr", None)

        if filter_expr:
            schema_prefix = (self._schema,) if self._schema else ()
            table_identifier = sql.Identifier(
                *schema_prefix, self._table + self.CONTENT_TABLE
            )
            key, value = (x.strip() for x in filter_expr.split("="))
            where_sql = sql.SQL(
                """
                WHERE doc_id in (
                    SELECT doc_id FROM {table_identifier} WHERE
                    json_lookup(metadata, '/{key}', 'jpointer_simdjson') = {value}
                )
            """
            ).format(
                table_identifier=table_identifier,
                key=sql.SQL(key),
                value=sql.SQL(value),
            )
        elif delete_all:
            where_sql = sql.SQL(
                """
                WHERE 1=1
            """
            )
        elif ids is not None:
            uuids = tuple(sql.Literal(id) for id in ids)
            ids_formatted = sql.SQL(", ").join(uuids)
            where_sql = sql.SQL(
                """
                WHERE doc_id IN ({ids})
            """
            ).format(
                ids=ids_formatted,
            )
        else:
            raise ValueError("Either ids or delete_all must be provided.")

        schema_prefix = (self._schema,) if self._schema else ()
        with self.connection.get_cursor() as cursor:
            table_identifier = sql.Identifier(
                *schema_prefix, self._table + self.CONTENT_TABLE
            )
            query = sql.SQL("DELETE FROM {table} {where_sql}").format(
                table=table_identifier, where_sql=where_sql
            )
            cursor.execute(query)

            table_identifier = sql.Identifier(*schema_prefix, self._table)
            query = sql.SQL("DELETE FROM {table} {where_sql}").format(
                table=table_identifier, where_sql=where_sql
            )
            cursor.execute(query)

            if self._table_exists(
                cursor, self._table + self.LSH_INDEX_TABLE, *schema_prefix
            ):
                table_identifier = sql.Identifier(
                    *schema_prefix, self._table + self.LSH_INDEX_TABLE
                )
                query = sql.SQL("DELETE FROM {table} {where_sql}").format(
                    table=table_identifier, where_sql=where_sql
                )
                cursor.execute(query)


class Yellowbrick(VectorStore):

    def __init__(self, system: System):
        super().__init__(system)

        self.yellowbrick = {}
        self.connection_string = os.environ.get("YELLOWBRICK_DB_CONNECTION_STRING")
        if self.connection_string is None:
            raise ValueError(
                "YELLOWBRICK_DB_CONNECTION_STRING environment variable not set"
            )

    def get_collection(
        self, collection: str, db_connection_id: str = None
    ) -> YellowbrickDataherald:
        if db_connection_id is not None and collection not in self.yellowbrick:
            db_connection_repository = DatabaseConnectionRepository(
                self.system.instance(DB)
            )
            database_connection = db_connection_repository.find_by_id(db_connection_id)
            _embedding = OpenAIEmbeddings(
                openai_api_key=database_connection.decrypt_api_key(),
                model=EMBEDDING_MODEL,
            )
            _yellowbrick = YellowbrickDataherald(
                _embedding, self.connection_string, collection
            )
            self.yellowbrick[collection] = _yellowbrick

        return self.yellowbrick[collection]

    @override
    def query(
        self,
        query_texts: List[str],
        db_connection_id: str,
        collection: str,
        num_results: int,
    ) -> list:
        target_collection = self.get_collection(collection, db_connection_id)
        results = []
        for query_text in query_texts:
            xq = target_collection._embedding.embed_query(query_text)
            query_results = target_collection.similarity_search_with_score_by_vector(
                xq, num_results
            )
            results.extend(
                self.convert_and_filter_results(query_results, db_connection_id)
            )

        return results

    @override
    def add_records(self, golden_sqls: List[GoldenSQL], collection: str):
        for golden_sql in golden_sqls:
            self.add_record(
                golden_sql.prompt_text,
                golden_sql.db_connection_id,
                collection,
                [
                    {
                        "tables_used": (
                            ", ".join(Parser(golden_sql.sql))
                            if isinstance(Parser(golden_sql.sql), list)
                            else ""
                        ),
                        "db_connection_id": str(golden_sql.db_connection_id),
                    }
                ],
                ids=[str(golden_sql.id)],
            )

    @override
    def add_record(
        self,
        documents: str,
        db_connection_id: str,
        collection: str,
        metadata: Any,
        ids: List,
    ):
        if not metadata:
            metadata = [{}]
        metadata[0].update({"dataherald_id": ids[0]})

        self.get_collection(collection, db_connection_id).add_texts(
            [documents], metadata
        )

    @override
    def delete_record(self, collection: str, id: str):
        filter_expr = f"dataherald_id = '{id}'"
        self.get_collection(collection).delete(filter_expr=filter_expr)

    @override
    def delete_collection(self, collection: str):
        self.get_collection(collection).delete(delete_all=True)

    @override
    def create_collection(self, collection: str):
        super().create_collection(collection)

    def convert_and_filter_results(
        self,
        yellowbrick_results: List[Tuple[Document, float]],
        db_connection_id: str,
    ) -> List:
        results = []
        for doc, score in yellowbrick_results:
            if doc.metadata.get("db_connection_id") == db_connection_id:
                results.append(
                    {
                        "id": doc.metadata.get("dataherald_id"),
                        "score": score,
                    }
                )
        return results
