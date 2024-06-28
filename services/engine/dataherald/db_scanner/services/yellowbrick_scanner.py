from datetime import datetime, timedelta
from overrides import override
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql import text

from dataherald.db_scanner.models.types import QueryHistory
from dataherald.db_scanner.services.abstract_scanner import AbstractScanner
from dataherald.sql_database.base import SQLDatabase

MIN_CATEGORY_VALUE = 1
MAX_CATEGORY_VALUE = 100
MAX_LOGS = 5000


class YellowbrickScanner(AbstractScanner):
    @override
    def cardinality_values(self, column: Column, db_engine: SQLDatabase) -> list | None:
        rs = db_engine.engine.execute(
            f"SELECT n_distinct, most_common_vals::TEXT::TEXT[] FROM pg_catalog.pg_stats WHERE tablename = '{column.table.name}' AND attname = '{column.name}'"  # noqa: S608 E501
        ).fetchall()

        if (
            len(rs) > 0
            and MIN_CATEGORY_VALUE < rs[0]["n_distinct"] <= MAX_CATEGORY_VALUE
        ):
            return rs[0]["most_common_vals"]
        return None

    @override
    def get_logs(
        self, table: str, db_engine: SQLDatabase, db_connection_id: str
    ) -> list[QueryHistory]:
        database_name = db_engine.engine.url.database.split("/")[0]
        filter_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        print(f"GET_LOGS() SCANNING yellowbrick query logs for {database_name} dialect {db_engine.engine.dialect}")
        sql_query = text(
            """
            SELECT query_text, username, count(*) as occurrences 
            FROM sys.log_query 
            WHERE database_name = :database_name 
            AND error_code = '00000' 
            AND submit_time > :filter_date 
            AND query_text ILIKE :from_table 
            AND query_text ILIKE '%SELECT%' 
            GROUP BY query_text, username 
            ORDER BY occurrences DESC 
            LIMIT :max_logs
            """
        )
        params = {
            'database_name': database_name,
            'filter_date': filter_date,
            'from_table': f'%FROM {table}%',
            'max_logs': MAX_LOGS
        }
        rows = db_engine.engine.execute(sql_query, **params).fetchall()
        return [
            QueryHistory(
                db_connection_id=db_connection_id,
                table_name=table,
                query=row[0],
                user=row[1],
                occurrences=row[2],
            )
            for row in rows
        ]
