#!/usr/bin/python
# -*- coding: utf-8 -*-

from typing import Dict
from urllib.parse import quote
import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, inspect, Table, MetaData, text
from .sql_connector_utils import cast_sql_to_typescript_types
from cmr_connectors_lib.connector import Connector
from loguru import logger

class SqlConnector(Connector):

    def __init__(self, host, user, password, port, database):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.database = database
        self.driver = None

    def get_engine(self):
        engine = create_engine(
            f"{self.driver}://{quote(self.user, safe='/')}:{quote(self.password, safe='/')}@{self.host}:{self.port}"
            f"/{self.database}", echo=False)

        return engine
    
    def get_connection(self):
        """Returns a connection object from the engine."""
        return self.get_engine().connect()
    
    def ping(self):
        """Returns True if the connection is successful, False otherwise."""
        try:
            with self.get_connection() as conn:  # Use context manager to handle connection cleanup
                result = conn.execute(text("SELECT 1"))  # Execute the query
                result.fetchone()  # Ensure the query runs
                logger.info("Database connection is active.")
                return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def get_connection_tables(self):
        """Returns a list of all table names in the database."""
        engine = self.get_engine()
        inspector = inspect(engine)
        return inspector.get_table_names()
    
    def get_connection_columns(self, table_name: str):
        """
        Returns a list of all column names in the table.
        Supports schema-qualified table names like 'schema.table'.
        """
        engine = self.get_engine()
        inspector = inspect(engine)

        # Handle schema-qualified names
        if '.' in table_name:
            schema, table = table_name.split('.', 1)
        else:
            schema, table = None, table_name

        columns = [
            {'name': col['name'], 'type': cast_sql_to_typescript_types(col['type'])}
            for col in inspector.get_columns(table, schema=schema)
        ]
        return columns

    def get_df(self, query, preview=False, rows=10, *args, **kwargs):
        constructed_query = self.construct_query(query, preview, rows)
        conn = self.get_engine()
        return pd.read_sql_query(constructed_query, conn)

    def upload_df(self, df, table, schema, if_exists='fail', *args, **kwargs):
        conn = self.get_engine()
        df.to_sql(table, con=conn, index=False, if_exists=if_exists)

    def construct_query(self, query, preview, rows):
        return query
    
    
    def get_database_schema(self) -> Dict[str, Table]:
        try:
            connection = self.get_connection()
            metadata = MetaData()
            metadata.reflect(bind=connection)
            return metadata.tables
        except Exception as reflect_err:
            logger.error(f"Schema reflection error: {reflect_err}")
            raise ValueError(f"Failed to retrieve database schema: {str(reflect_err)}")
