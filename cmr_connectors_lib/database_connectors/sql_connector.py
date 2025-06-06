#!/usr/bin/python
# -*- coding: utf-8 -*-

from abc import abstractmethod

from pyodbc import Cursor


class SqlConnector():

    def __init__(self, host, user, password, port, database):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.database = database
        self.driver = None

    
    @abstractmethod
    def get_connection(self):
        pass
    
    @abstractmethod
    def ping(self):
        pass
    
    @abstractmethod
    def get_connection_tables(self):
        """
        Returns a list of all table names in the given PostgreSQL schema.
        """
        pass
    
    def get_connection_columns(self, table_name, schema=None):
        """
        Returns a list of dicts with column names and mapped TypeScript types
        for the given Postgres table in the given schema.

        Args:
            table_name: The name of the table to get columns for
            schema: Schema name for postgres
        """
        pass

    
    def get_database_schema(self):
        pass

    def extract_data_batch(self, table_name: str, offset: int = 0, limit: int = 100):
        """
           Extracts a batch of rows from a table using SKIP/FIRST.
           Defaults to the first 100 rows if offset/limit are not provided.
        """
        pass

    def fetch_batch(self, cursor: Cursor, table_name, offset: int, limit: int = 100):
        """
          Fetch up to `limit` rows from `table`, skipping the first `offset` rows.

        Args:
            table_name (str):       Name of the Informix table.
            offset (int):      Number of rows to skip.
            limit (int):       Maximum rows to return.
            cursor:            Cursor.

        Returns:
            list of tuple:     The fetched rows, empty if none remain.
        """
        pass


    def extract_table_schema(self, table_name: str, schema: str = "public"):
        """
           Gather column-level details from database including:
           - column name, type, nullability, default
           - primary key, foreign key, and index flags
        """
        pass