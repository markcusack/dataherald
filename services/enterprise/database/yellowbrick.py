from sqlalchemy import (
    create_engine, Column, Integer, String, Table, MetaData, Sequence, insert, select, update, delete, func, text, literal_column, desc, asc, and_, or_
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import column
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.sql.expression import label
from config import db_settings, TABLE_DESCRIPTION_COL, PROMPT_COL, NL_GENERATION_COL, SQL_GENERATION_COL
from bson.objectid import ObjectId

import re
import datetime

class Yellowbrick:
    db_uri = db_settings.yellowbrick_uri
    engine = create_engine(db_uri)
    metadata = MetaData(bind=engine)
    Session = sessionmaker(bind=engine)
    
    @classmethod
    def create_jsonb_table(cls, table_name: str):
        id_sequence = Sequence(f"{table_name}_id_seq")
        Table(
            table_name,
            cls.metadata,
            Column(
                "id",
                Integer,
                id_sequence,
                primary_key=True,
                server_default=id_sequence.next_value(),
            ),
            Column("data", JSONB),
            extend_existing=True,
        ).create(cls.engine)

    @classmethod
    def get_table(cls, table_name: str):
        from sqlalchemy import inspect

        inspector = inspect(cls.engine)
        if not inspector.has_table(table_name):
            cls.create_jsonb_table(table_name)
        return Table(table_name, cls.metadata, extend_existing=True, autoload_with=cls.engine)

    @classmethod
    def find_one(cls, table_name: str, query: dict, sort: list = None) -> dict:
        table = cls.get_table(table_name)
        with cls.Session() as session:
            stmt = select(table.c.data)

            if query:
                for key, value in query.items():
                    if isinstance(value, dict) and "$exists" in value:
                        if value["$exists"]:
                            condition = text(f"JSON_TYPEOF(data:{key} null on error) IS NOT NULL")
                        else:
                            condition = text(f"JSON_TYPEOF(data:{key} null on error) IS NULL")
                        stmt = stmt.where(condition)
                    else:
                        param_name = f"param_{key.replace('.', '_')}"
                        condition = text(f"data:{key} null on error = :{param_name}")
                        formatted_value = value if isinstance(value, (int, float, bool)) else f'"{value}"'
                        stmt = stmt.where(condition.bindparams(**{param_name: formatted_value}))

            if sort:
                for column, order in sort:
                    order = "DESC" if order == 1 else "ASC"
                    stmt = stmt.order_by(text(f"data:{column} {order}"))

            result = session.execute(stmt).fetchone()
            return result.data if result else None

    @classmethod
    def preprocess_json(cls, data):
        if isinstance(data, dict):
            return {k: cls.preprocess_json(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [cls.preprocess_json(i) for i in data]
        elif isinstance(data, datetime.datetime):
            return data.isoformat()
        return data
    
    @classmethod
    def merge_json(cls, base: dict, updates: dict) -> dict:
        for key, value in updates.items():
            if '.' in key:
                keys = key.split('.')
                current = base
                for k in keys[:-1]:
                    if k not in current or not isinstance(current[k], dict):
                        current[k] = {}
                    current = current[k]
                current[keys[-1]] = value
            else:
                base[key] = value
        return base
    
    @classmethod
    def insert_one(cls, table_name: str, obj: dict) -> int:
        table = cls.get_table(table_name)
        obj = cls.preprocess_json(obj)
        generated_oid = str(ObjectId())
        obj["_id"] = generated_oid
        with cls.Session() as session:
            filtered_obj = {"data": obj}
            new_entry = table.insert().values(**filtered_obj)
            result = session.execute(new_entry)
            session.commit()
            return generated_oid

    @classmethod
    def insert_many(cls, table_name: str, objs: list[dict]) -> list[str]:
        table = cls.get_table(table_name)
        session = cls.Session()
        generated_oids = []
        try:
            for obj in objs:
                obj = cls.preprocess_json(obj)
                generated_oid = str(ObjectId())
                obj["_id"] = generated_oid
                filtered_obj = {"data": obj}
                new_entry = table.insert().values(**filtered_obj)
                session.execute(new_entry)
                generated_oids.append(generated_oid)

            session.commit()
        except Exception as e:
            session.rollback()
            print(f"An error occurred: {e}")
            raise
        finally:
            session.close()

        return generated_oids

    @classmethod
    def update_one(cls, table_name: str, query: dict, obj: dict) -> int:
        table = cls.get_table(table_name)
        existing_row = cls.find_one(table_name, query)
        if existing_row:
            with cls.Session() as session:
                obj = cls.preprocess_json(obj)
                existing_row = cls.merge_json(existing_row, obj)
                updated_obj = {"data": existing_row}
                conditions = []
                for key, value in query.items():
                    param_name = f"param_{key.replace('.', '_')}"
                    condition = text(f"data:{key} null on error = :{param_name}")
                    formatted_value = value if isinstance(value, (int, float, bool)) else f'"{value}"'
                    conditions.append(condition.bindparams(**{param_name: formatted_value}))  

                result = session.execute(table.update().where(*conditions).values(**updated_obj))
                session.commit()
                return result.rowcount

    @classmethod
    def update_many(cls, table_name: str, filter: dict, obj: dict) -> int:
        table = cls.get_table(table_name)
        session = cls.Session()
        obj = cls.preprocess_json(obj)
        updated_obj = {"data": obj}
        try:
            conditions = []
            for key, value in filter.items():
                formatted_value = value if isinstance(value, (int, float, bool)) else f'"{value}"'
                condition = text(f"data:{key} null on error = :{key}").bindparams(**{key: formatted_value})
                conditions.append(condition)

            update_stmt = update(table).where(*conditions).values(**updated_obj)
            result = session.execute(update_stmt)
            session.commit()
            row_count = result.rowcount
            return row_count
        except Exception as e:
            session.rollback()
            print(f"Error during update: {e}")
            raise
        finally:
            session.close()

    @classmethod
    def find_by_id(cls, table_name: str, id: int) -> dict:
        table = cls.get_table(table_name)
        with cls.Session() as session:
            stmt = select(table.c.data)
            if id:
                conditions = []
                formatted_value = f'"{id}"'
                conditions.append(text(f"data:_id = :value").bindparams(value=formatted_value))
                stmt = stmt.where(*conditions)
            result = session.execute(stmt).fetchone()
            return result.data if result else None
        
    @classmethod
    def find_by_object_ids(cls, table_name: str, ids: list[str]) -> list[dict]:
        table = cls.get_table(table_name)
        session = cls.Session()
        try:
            stmt = select(table.c.data)
            formatted_ids = ', '.join(f"'{id}'" for id in ids)
            condition = text(f"data:_id IN ({formatted_ids})")
            stmt = stmt.where(condition)
            results = session.execute(stmt).fetchall()
            return [dict(result) for result in results] if results else []
        except Exception as e:
            print(f"Error fetching data: {e}")
            raise
        finally:
            session.close()

    @classmethod
    def find(cls, table_name: str, query: dict) -> list[dict]:
        table = cls.get_table(table_name)
        with cls.Session() as session:
            stmt = select(table.c.data)
            if query:
                for key, value in query.items():
                    param_name = f"param_{key.replace('.', '_')}"
                    condition = text(f"data:{key} null on error = :{param_name}")
                    formatted_value = value if isinstance(value, (int, float, bool)) else f'"{value}"'
                    stmt = stmt.where(condition.bindparams(**{param_name: formatted_value}))
            results = session.execute(stmt).fetchall()
        return [result.data for result in results] if results else []
        
    @classmethod
    def delete_one(cls, table_name: str, query: dict) -> int:
        table = cls.get_table(table_name)
        with cls.Session() as session:  
            conditions = []
            for key, value in query.items():
                param_name = f"param_{key.replace('.', '_')}"
                condition = text(f"data:{key} null on error = :{param_name}")
                formatted_value = value if isinstance(value, (int, float, bool)) else f'"{value}"'
                conditions.append(condition.bindparams(**{param_name: formatted_value}))   
            result = session.execute(table.delete().where(*conditions))
            session.commit()
            return result.rowcount

    @classmethod
    def aggregate(cls, table_name: str, pipeline: list) -> list[dict]:
        raise NotImplementedError("Aggregation needs to be implemented based on the specific use case.")
    
    @classmethod
    def get_table_description_grouped_by_db_connection_id(
        cls, tables_description_ids: list[str]
    ) -> list[dict]:
        table = cls.get_table(TABLE_DESCRIPTION_COL)
        session = cls.Session()
        try:
            db_conn_id_col = literal_column("data:db_connection_id")
            formatted_ids = ', '.join(f"'\"{id}\"'" for id in tables_description_ids)
            condition = text(f"data:_id IN ({formatted_ids})")

            stmt = (
                select(
                    db_conn_id_col.label("_id"),
                    func.count().label("count"),
                    (
                        func.concat(
                            literal_column("'['"), 
                            func.string_agg(table.c.data.cast(String), literal_column("','")),
                            literal_column("']'")
                        )
                    ).label("documents")
                )
                .select_from(table)
                .where(condition)
                .group_by(db_conn_id_col)
            )
            rows = session.execute(stmt).fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            print(f"Error in grouped query: {e}")
            raise
        finally:
            session.close()

    @classmethod
    def get_generation_aggregations(
        cls,
        skip: int,
        limit: int,
        order: str,
        ascend: bool,
        org_id: str,
        search_term: str = "",
        db_connection_id: str = None,
    ):
        prompts_table = cls.get_table(PROMPT_COL)
        nl_generations_table = cls.get_table(NL_GENERATION_COL)
        sql_generations_table = cls.get_table(SQL_GENERATION_COL)
        session = cls.Session()
        if org_id:
            org_id = f'"{org_id}"'
        if db_connection_id:
            db_connection_id = f'"{db_connection_id}"'

        search_term = re.escape(search_term)
        sql_prompt_id_col = literal_column("sql_generations.data:prompt_id")
        sql_created_at_col = literal_column("sql_generations.data:created_at")

        from sqlalchemy import over, case
        row_number_sql = func.row_number().over(partition_by=sql_prompt_id_col, order_by=desc(sql_created_at_col))
        latest_sql_subq = (
            select(sql_generations_table.c.data.label("sql_data"))
            .select_from(sql_generations_table)
            .add_columns(row_number_sql.label("rn"))
            .cte("latest_sql_subq")
        )

        latest_sql_final = (
            select(latest_sql_subq.c.sql_data)
            .where(latest_sql_subq.c.rn == 1)
            .cte("latest_sql_final")
        )

        nl_sql_gen_id_col = literal_column("nl_generations.data:sql_generation_id")
        nl_created_at_col = literal_column("nl_generations.data:created_at")
        row_number_nl = func.row_number().over(partition_by=nl_sql_gen_id_col, order_by=desc(nl_created_at_col))
        latest_nl_subq = (
            select(nl_generations_table.c.data.label("nl_data"))
            .select_from(nl_generations_table)
            .add_columns(row_number_nl.label("rn"))
            .cte("latest_nl_subq")
        )

        latest_nl_final = (
            select(latest_nl_subq.c.nl_data)
            .where(latest_nl_subq.c.rn == 1)
            .cte("latest_nl_final")
        )

        prompt_id_col = literal_column("prompts.data:_id")
        prompt_org_id_col = literal_column("prompts.data:metadata.dh_internal.organization_id")
        prompt_text_col = literal_column("prompts.data:text")
        prompt_db_conn_id_col = literal_column("prompts.data:db_connection_id")
        prompt_metadata_col = literal_column("prompts.data:metadata")

        sql_data_prompt_id = literal_column("latest_sql_final.sql_data:prompt_id")
        sql_data_id = literal_column("latest_sql_final.sql_data:_id")
        nl_data_sql_gen_id = literal_column("latest_nl_final.nl_data:sql_generation_id")

        stmt = (
            select(
                prompt_id_col.label("_id"),
                prompt_text_col.label("text"),
                prompt_db_conn_id_col.label("db_connection_id"),
                prompt_metadata_col.label("metadata"),
                sql_data_id.label("sql_generation_id"),
                literal_column("latest_sql_final.sql_data:sql").label("sql"),
                literal_column("latest_nl_final.nl_data:_id").label("nl_generation_id"),
            )
            .select_from(
                prompts_table
                .join(
                    latest_sql_final,
                    text(f"(latest_sql_final.sql_data:prompt_id) = {prompt_id_col}")
                )
                .join(
                    latest_nl_final,
                    text(f"(latest_nl_final.nl_data:sql_generation_id) = {sql_data_id}"),
                    isouter=True
                )
            )
            .where(prompt_org_id_col == org_id)
        )

        if db_connection_id:
            stmt = stmt.where(prompt_db_conn_id_col == db_connection_id)

        if search_term:
            stmt = stmt.where(
                or_(
                    prompt_text_col.ilike(f"%{search_term}%"),
                    literal_column("(latest_sql_final.sql_data:sql)").ilike(f"%{search_term}%")
                )
            )

        order_col = literal_column(f"prompts.data:{order}")
        stmt = stmt.order_by(order_col.asc() if ascend else order_col.desc())
        stmt = stmt.offset(skip).limit(limit)

        try:
            rows = session.execute(stmt).fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            print(f"Error in grouped query: {e}")
            raise
        finally:
            session.close()
