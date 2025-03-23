#!/usr/bin/python
# -*- coding: utf-8 -*-

from typing import Dict
from .sql_connector import SqlConnector
from .sql_connector_utils import cast_informix_to_typescript_types
import pyodbc
from loguru import logger

class InformixConnector(SqlConnector):

    def __init__(self, host, user, password, port, database, protocol):
        super().__init__(host, user, password, port, database)
        self.protocol = protocol
        #TODO: Use custom path
        self.driver_path = "app/main/drivers/ddifcl28.so"

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
        
        for row in cursor.fetchall():
            logger.info("colname: {}, coltype: {}", row.colname, row.coltype)

        columns = [{'name': row.colname, 'type': cast_informix_to_typescript_types(row.coltype)} for row in cursor.fetchall()]
    
        cursor.close()
        return columns
    
    def get_database_schema(self) -> Dict[str, Dict]:
        try:
            # TODO: Use the connector to get the schema based on the connector type
            logger.debug("Getting database schema for Informix using pyodbc")
            tables = {}
            cursor = self.get_connection().cursor()
            
            # First, get the current database
            try:
                cursor.execute("SELECT TRIM(DBINFO('dbname')) FROM systables WHERE tabid = 1")
                current_db = cursor.fetchone()[0]
                logger.debug(f"Current database: {current_db}")
            except Exception as db_err:
                logger.warning(f"Could not get current database: {db_err}")
                current_db = None
            
            # Get all user tables from Informix with more inclusive query
            table_query = """
                SELECT DISTINCT
                    t.tabname,
                    t.owner
                FROM systables t
                WHERE t.tabtype = 'T'
                AND t.tabid >= 100  -- User tables typically start from 100
            """
            logger.debug(f"Executing table query: {table_query}")
            cursor.execute(table_query)
            table_rows = cursor.fetchall()
            
            logger.debug(f"Found {len(table_rows)} tables")
            
            for table_row in table_rows:
                table_name = table_row[0].strip()
                owner = table_row[1].strip() if table_row[1] else None
                
                logger.debug(f"Processing table: {table_name}, owner: {owner}")
                
                # Get columns for each table
                column_query = f"""
                    SELECT 
                        c.colname,
                        c.coltype,
                        c.collength
                    FROM syscolumns c
                    JOIN systables t ON c.tabid = t.tabid
                    WHERE t.tabname = '{table_name}'
                    ORDER BY c.colno
                """
                
                try:
                    logger.debug(f"Getting columns for table {table_name}")
                    cursor.execute(column_query)
                    columns = cursor.fetchall()
                    
                    if columns:
                        table_info = {
                            'name': table_name,
                            'owner': owner,
                            'columns': [{
                                'name': col[0].strip(),
                                'type': cast_informix_to_typescript_types(col[1]),
                                'length': col[2]
                            } for col in columns]
                        }
                        tables[table_name] = table_info
                        logger.debug(f"Added table {table_name} with {len(columns)} columns")
                except Exception as col_err:
                    logger.error(f"Error getting columns for table {table_name}: {col_err}")
                    continue
                
            logger.debug(f"Retrieved schema for {len(tables)} tables")
            if len(tables) == 0:
                logger.warning("No tables found in the schema")
                
            return tables
        except Exception as e:
            logger.error(f"Error getting database schema: {str(e)}")
            raise ValueError(f"Failed to retrieve database schema: {str(e)}")


