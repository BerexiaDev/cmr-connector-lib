#!/usr/bin/python
# -*- coding: utf-8 -*-

import pyodbc
from typing import Dict
from loguru import logger
from .sql_connector import SqlConnector
from .sql_connector_utils import cast_informix_to_typescript_types, cast_informix_to_postgresql_type, safe_convert_to_string
class InformixConnector(SqlConnector):

    def __init__(self, host, user, password, port, database, protocol, locale):
        super().__init__(host, user, password, port, database)
        self.protocol = protocol
        self.locale = locale
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
            f"CLIENT_LOCALE={self.locale};"   # Explicitly set client locale
            f"DB_LOCALE={self.locale};"       # Explicitly set database locale
         )
        conn = pyodbc.connect(conn_str)
        # Set decoding to ISO-8859-1 (en_US.819) for both SQL_CHAR and SQL_WCHAR
        conn.setdecoding(pyodbc.SQL_CHAR, encoding='latin1')  # ISO-8859-1 = Latin-1
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')  # In case of wide chars
        return conn

    def ping(self):
        """Returns True if the connection is successful, False otherwise."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()  # Ensure the query runs
            logger.info("Database connection is active.")
            cursor.close()
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    
    def extract_data_batch(self, table_name: str, offset: int = 0, limit: int = 100):
        """
           Extracts a batch of rows from an Informix table using SKIP/FIRST.
           Defaults to the first 100 rows if offset/limit are not provided.
        """
        query = f'SELECT * FROM "{table_name}" SKIP {offset} FIRST {limit}'
        logger.info(f"Fetching batch: table={table_name}, offset={offset}, limit={limit}")
        
        try:
            connection = self.get_connection()
            result_proxy = connection.execute(query)
            rows = result_proxy.fetchall()
            column_names = [col[0] for col in result_proxy.description]
            batch_data = [
                {col: safe_convert_to_string(row[i]) for i, col in enumerate(column_names)}
                for row in rows
            ]

            return batch_data

        except Exception as e:
            logger.error(f"Error extracting batch from {table_name}: {str(e)}")
            return [] 
    
    
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
        rows = cursor.fetchall()
        
        for row in rows:
            logger.info("colname: {}, coltype: {}", row.colname, row.coltype)

        columns = [{'name': row.colname, 'type': cast_informix_to_typescript_types(row.coltype)} for row in rows]
    
        cursor.close()
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
        
        
    def extract_table_schema(self, table_name):
        try:
            query = f'''
            SELECT 
            c.colno AS ordinal_position,
            c.colname,
            c.coltype,
            c.collength,
            CASE 
                WHEN BITAND(c.coltype, 256) = 256 THEN 'NO' 
                ELSE 'YES' 
            END AS is_nullable,
            
            CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM sysconstraints sc
                    JOIN sysindexes si ON sc.idxname = si.idxname
                    WHERE sc.constrtype = 'P'
                        AND sc.tabid = c.tabid
                        AND (
                            si.part1 = c.colno OR si.part2 = c.colno OR si.part3 = c.colno OR 
                            si.part4 = c.colno OR si.part5 = c.colno OR si.part6 = c.colno OR 
                            si.part7 = c.colno OR si.part8 = c.colno OR si.part9 = c.colno OR 
                            si.part10 = c.colno OR si.part11 = c.colno OR si.part12 = c.colno OR 
                            si.part13 = c.colno OR si.part14 = c.colno OR si.part15 = c.colno OR 
                            si.part16 = c.colno
                        )
                ) THEN 'YES'
                ELSE 'NO'
            END AS is_primary_key,
            
            CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM sysconstraints sc
                    JOIN sysreferences sr ON sc.constrid = sr.constrid
                    JOIN sysindexes si ON sc.idxname = si.idxname
                    WHERE sc.constrtype = 'R'
                        AND sc.tabid = c.tabid
                        AND (
                            si.part1 = c.colno OR si.part2 = c.colno OR si.part3 = c.colno OR 
                            si.part4 = c.colno OR si.part5 = c.colno OR si.part6 = c.colno OR 
                            si.part7 = c.colno OR si.part8 = c.colno OR si.part9 = c.colno OR 
                            si.part10 = c.colno OR si.part11 = c.colno OR si.part12 = c.colno OR 
                            si.part13 = c.colno OR si.part14 = c.colno OR si.part15 = c.colno OR 
                            si.part16 = c.colno
                        )
                ) THEN 'YES'
                ELSE 'NO'
            END AS is_foreign_key,
            
            d.default AS default_value

            FROM syscolumns c
            JOIN systables t ON c.tabid = t.tabid
            LEFT JOIN sysdefaults d ON c.tabid = d.tabid AND c.colno = d.colno
            WHERE t.tabname = '{table_name}'
            ORDER BY c.colno;
            '''
            
            cursor = self.get_connection().cursor()
            cursor.execute(query)
            columns = cursor.fetchall()
            cursor.close()
            
            column_list = []
            for col in columns:
                column_list.append({
                    'position': col[0],
                    'name': col[1],
                    'type': cast_informix_to_postgresql_type(col[2]),
                    'length': col[3],
                    'nullable': col[4],
                    'primary_key': col[5],
                    'foreign_key': col[6],
                    'default': col[7]
                })
            
            return column_list
        
        except Exception as e:
            logger.error(f"Error getting database schema: {str(e)}")
            return {"status": "error", "message": f"An error occurred: {str(e)}"}, 500
    