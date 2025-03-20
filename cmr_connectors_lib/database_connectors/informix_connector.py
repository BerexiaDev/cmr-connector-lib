#!/usr/bin/python
# -*- coding: utf-8 -*-

from .sql_connector import SqlConnector
from .sql_connector_utils import cast_informix_to_typescript_types
import pyodbc
import os


class InformixConnector(SqlConnector):

    def __init__(self, host, user, password, port, database, protocol):
        super().__init__(host, user, password, port, database)
        self.protocol = protocol
        # Get the path relative to the virtual environment
        venv_path = os.environ.get('VIRTUAL_ENV')
        if venv_path:
            self.driver_path = os.path.join(venv_path, 'app/main/drivers/ddifcl28.so')
        else:
            # Fallback to the current working directory
            self.driver_path = os.path.abspath('app/main/drivers/ddifcl28.so')

    def construct_query(self, query, preview, rows):
        if preview:
            query = query.lower()
            if "first" not in query:
                query = query.replace(";", " ")
                query = f"SELECT FIRST {rows} * FROM ({query})"
        return query

    def get_connection(self):
        """Returns a connection object from the driver."""
        conn_str = (
            f"DRIVER={self.driver_path};"
            f"DATABASE={self.database};"
            f"HOSTNAME={self.host};"
            f"PORT={self.port};"
            f"PROTOCOL={self.protocol};"
            f"UID={self.user};"
            f"PWD={self.password};"
        )
        conn = pyodbc.connect(conn_str)
        conn.setdecoding(pyodbc.SQL_CHAR, encoding="utf-8")
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-8")
        return conn

    def get_connection_tables(self):
        """Returns a list of all table names in the cmr database."""
        cursor = self.get_connection().cursor()
        cursor.execute("SELECT tabname FROM systables WHERE tabtype = 'T' AND tabname NOT LIKE 'sys%'")
        tables = [row.tabname for row in cursor.fetchall()]
        cursor.close()
        return tables

    def get_connection_columns(self, table_name):
        """Returns a list of dictionaries with column names and types for the given table."""
        cursor = self.get_connection().cursor()
    
        cursor.execute(f"""
        SELECT colname, coltype 
        FROM syscolumns 
        WHERE tabid = (SELECT tabid FROM systables WHERE tabname = '{table_name}')
    """)

        columns = [{'name': row.colname, 'type': cast_informix_to_typescript_types(row.coltype)} for row in cursor.fetchall()]
    
        cursor.close()
        return columns


