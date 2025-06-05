#!/usr/bin/python
# -*- coding: utf-8 -*-

import pyodbc
from loguru import logger
from .sql_connector import SqlConnector


class SqlServerConnector(SqlConnector):

    def __init__(self, host, user, password, port, database):
        super().__init__(host, user, password, port, database)
        self.driver = "mssql+pyodbc"
        
    def get_connection_string(self):
        return (f"mssql+pyodbc://{self.user}:{self.password}@{self.host}:{self.port}/"
                f"{self.database}?driver=ODBC+Driver+17+for+SQL+Server")

    def get_connection(self):
        """Returns a pyodbc connection object directly."""
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.host},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.user};"
            f"PWD={self.password};"
        )
        return pyodbc.connect(conn_str)

    def ping(self):
        """Returns True if the connection is successful, False otherwise."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()  # Ensure the query runs
            logger.info("Database connection is active.")
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False

    def construct_query(self, query, preview, rows):
        if preview:
            query = query.lower()
            query = query.replace("select", "select " + " top " + str(rows))
        return query