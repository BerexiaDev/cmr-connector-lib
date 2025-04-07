#!/usr/bin/python
# -*- coding: utf-8 -*-

import pyodbc
from typing import Dict
from .sql_connector import SqlConnector
from .sql_connector_utils import cast_informix_to_typescript_types
from loguru import logger
class InformixConnector(SqlConnector):
    from sqlalchemy import create_engine

    def __init__(self, host, user, password, port, database, protocol, locale):
        super().__init__(host, user, password, port, database)
        self.protocol = protocol
        self.locale = locale
        self.driver = "ibm_db_sa"

    def construct_query(self, query, preview, rows):
        if preview:
            query = query.lower()
            if "first" not in query:
                query = query.replace(";", " ")
                query = f"SELECT FIRST {rows} * FROM ({query})"
        return query

    def get_engine(self):
        conn_str = (
            f"{self.driver}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database};"
            f"DELIMIDENT=Y;PROTOCOL={self.protocol};LOCALE={self.locale}"
        )
        engine = create_engine(conn_str)
        return engine

    def ping(self):
        """Returns True if the connection is successful, False otherwise."""
        try:
            conn = self.get_connection()
            conn.execute("SELECT 1")
            conn.fetchone()  # Ensure the query runs
            logger.info("Database connection is active.")
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def get_connection_tables(self):
        """Returns a list of all table names in the cmr database."""
        connection = self.get_connection()
        connection.execute("SELECT tabname FROM systables WHERE tabtype = 'T' AND tabname NOT LIKE 'sys%'")
        tables = [row.tabname for row in connection.fetchall()]
        connection.close()
        return tables

    def get_connection_columns(self, table_name):
        """Returns a list of dictionaries with column names and types for the given table."""
        connection = self.get_connection()
    
        connection.execute(f"""
            SELECT colname, coltype 
            FROM syscolumns 
            WHERE tabid = (SELECT tabid FROM systables WHERE tabname = '{table_name}')
        """)
        rows = connection.fetchall()
        
        for row in rows:
            logger.info("colname: {}, coltype: {}", row.colname, row.coltype)

        columns = [{'name': row.colname, 'type': cast_informix_to_typescript_types(row.coltype)} for row in rows]
    
        connection.close()
        return columns
    
    def count_table_rows(self, table_name: str) -> int:
        try:
            connection = self.get_connection()
            count_result = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            total_count = int(count_result[0]) if count_result else 0
            return total_count
        except Exception as e:
            logger.error(f"Error getting table total rows: {str(e)}")
            raise ValueError(f"Failed to get table total rows: {str(e)}")
    
    
    def get_database_schema(self) -> Dict[str, Dict]:
        try:
            # TODO: Use the connector to get the schema based on the connector type
            logger.debug("Getting database schema for Informix using pyodbc")
            tables = {}
            connection = self.get_connection()
            
            # First, get the current database
            try:
                connection.execute("SELECT TRIM(DBINFO('dbname')) FROM systables WHERE tabid = 1")
                current_db = connection.fetchone()[0]
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
            connection.execute(table_query)
            table_rows = connection.fetchall()
            
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
                    connection.execute(column_query)
                    columns = connection.fetchall()
                    
                    if columns:
                        # Build columns list while logging each column's info
                        column_list = []
                        for col in columns:
                            col_name = col[0].strip()
                            col_type = cast_informix_to_typescript_types(col[1])
                            logger.debug(f"Column {col_name} has type {col[1]} wihch is {col_type}")
                            column_list.append({
                                'name': col_name,
                                'type': col_type,
                                'length': col[2]
                            })
                            
                        table_info = {
                            'name': table_name,
                            'owner': owner,
                            'columns': column_list
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