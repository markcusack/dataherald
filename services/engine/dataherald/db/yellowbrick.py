from overrides import override
from sqlalchemy import Column, Integer, MetaData, Sequence, Table, create_engine, text, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker
from dataherald.config import System
from dataherald.db import DB
from bson.objectid import ObjectId

import datetime

class Yellowbrick(DB):
    def __init__(self, system: System):
        super().__init__(system)
        db_uri = system.settings.require("db_uri")
        self.engine = create_engine(db_uri)
        self.Session = sessionmaker(bind=self.engine)
        self.metadata = MetaData(bind=self.engine)

    def create_jsonb_table(self, table_name: str):
        id_sequence = Sequence(f"{table_name}_id_seq")
        Table(
            table_name,
            self.metadata,
            Column(
                "id",
                Integer,
                id_sequence,
                primary_key=True,
                server_default=id_sequence.next_value(),
            ),
            Column("data", JSONB),
            extend_existing=True,
        ).create(self.engine)

    def get_table(self, table_name: str):
        from sqlalchemy import inspect

        inspector = inspect(self.engine)
        if not inspector.has_table(table_name):
            self.create_jsonb_table(table_name)
        return Table(table_name, self.metadata, autoload_with=self.engine)

    def preprocess_json(self, data):
        if isinstance(data, dict):
            return {k: self.preprocess_json(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self.preprocess_json(i) for i in data]
        elif isinstance(data, datetime.datetime):
            return data.isoformat()
        return data

    def merge_json(self, base: dict, updates: dict) -> dict:
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
    
    @override
    def find_one(self, collection: str, query: dict) -> dict:
        table = self.get_table(collection)
        with self.Session() as session:
            stmt = select(table.c.data)
            if query:
                for key, value in query.items():
                    param_name = f"param_{key.replace('.', '_')}"
                    condition = text(f"data:{key} = :{param_name}")
                    formatted_value = value if isinstance(value, (int, float, bool)) else f'"{value}"'
                    stmt = stmt.where(condition.bindparams(**{param_name: formatted_value}))
            result = session.execute(stmt).fetchone()
            return result.data if result else None

    @override
    def insert_one(self, collection: str, obj: dict) -> int:
        table = self.get_table(collection)
        obj = self.preprocess_json(obj)
        generated_oid = str(ObjectId())
        obj["_id"] = generated_oid
        with self.Session() as session:
            obj = {"data": obj}
            new_entry = table.insert().values(**obj)
            result = session.execute(new_entry)
            session.commit()
            return generated_oid

    @override
    def rename(self, old_collection_name: str, new_collection_name) -> None:
        with self.engine.connect() as conn:
            conn.execute(f"ALTER TABLE {old_collection_name} RENAME TO {new_collection_name}")

    @override
    def rename_field(
        self, collection_name: str, old_field_name: str, new_field_name: str
    ) -> None:
        with self.engine.connect() as conn:
            conn.execute(
                f"ALTER TABLE {collection_name} RENAME COLUMN {old_field_name} TO {new_field_name}"
            )

    def update_one(self, table_name: str, query: dict, obj: dict, existing_row: dict) -> int:
        table = self.get_table(table_name)    
        with self.Session() as session:
            obj = self.preprocess_json(obj)
            existing_row = self.merge_json(existing_row, obj)
            updated_obj = {"data": existing_row}
            conditions = []
            for key, value in query.items():
                param_name = f"param_{key.replace('.', '_')}"
                condition = text(f"data:{key} = :{param_name}")
                formatted_value = value if isinstance(value, (int, float, bool)) else f'"{value}"'
                conditions.append(condition.bindparams(**{param_name: formatted_value}))  

            result = session.execute(table.update().where(*conditions).values(**updated_obj))
            session.commit()
            return existing_row["_id"]
        
    @override
    def update_or_create(self, collection: str, query: dict, obj: dict) -> int:
        existing_row = self.find_one(collection, query)
        if existing_row:
            return self.update_one(collection, query, obj, existing_row)
        else:
            return self.insert_one(collection, obj)

    @override
    def find_by_id(self, collection: str, id: str) -> dict:
        table = self.get_table(collection)
        with self.Session() as session:
            stmt = select(table.c.data)
            condition = text(f"data:_id = :value")
            formatted_value = f'"{id}"'
            stmt = stmt.where(condition.bindparams(value=formatted_value))   
            result = session.execute(stmt).fetchone()
            return result.data if result else {}

    @override
    def find(
        self,
        collection: str,
        query: dict,
        sort: list = None,
        page: int = 0,
        limit: int = 0,
    ) -> list:
        table = self.get_table(collection)
        with self.Session() as session:
            stmt = select(table.c.data)
            if query:
                for key, value in query.items():
                    param_name = f"param_{key.replace('.', '_')}"
                    condition = text(f"data:{key} = :{param_name}")
                    formatted_value = value if isinstance(value, (int, float, bool)) else f'"{value}"'
                    stmt = stmt.where(condition.bindparams(**{param_name: formatted_value}))
            if sort:
                for column, order in sort:
                    order = "DESC" if order == 1 else "ASC"
                    stmt = stmt.order_by(text(f"data:{column} {order}"))
            if limit > 0:
                offset = (page - 1) * limit if page > 0 else 0
                stmt = stmt.offset(offset).limit(limit)

            results = session.execute(stmt).fetchall()

        return [result.data for result in results] if results else []

    @override
    def find_all(self, collection: str, page: int = 0, limit: int = 0) -> list:
        table = self.get_table(collection)
        with self.Session() as session:
            stmt = select(table.c.data)
            if page > 0 and limit > 0:
                offset = (page - 1) * limit if page > 0 else 0
                stmt = stmt.offset(offset).limit(limit)
            results = session.execute(stmt).fetchall()

        return [result.data for result in results] if results else []       

    @override
    def delete_by_id(self, collection: str, id: str) -> int:
        table = self.get_table(collection)
        with self.Session() as session:      
            conditions = []
            formatted_value = f'"{id}"'
            conditions.append(text(f"data:_id = :value").bindparams(value=formatted_value))
            result = session.execute(
                table.delete().where(*conditions)
            )
            session.commit()
            return result.rowcount
