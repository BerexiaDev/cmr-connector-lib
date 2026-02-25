#!/usr/bin/python
# -*- coding: utf-8 -*-
from datetime import datetime
from typing import List, Dict, Any, Iterator

import jpype
import jaydebeapi
from loguru import logger

from .sql_connector import SqlConnector
from .sql_connector_utils import (
    cast_db2_as400_to_typescript_types,
    cast_db2_as400_to_postgresql_type,
    safe_convert_to_string,
)


class Db2As400Connector(SqlConnector):

    def __init__(self, host, user, password, schema):
        super().__init__(host, user, password, port=None, database=schema)
        self.schema = schema
        self.driver_path = "app/main/drivers/jt400-20.0.7.jar"

    def get_connection(self):
        if not jpype.isJVMStarted():
            jpype.startJVM(
                jpype.getDefaultJVMPath(),
                f"-Djava.class.path={self.driver_path}",
                "-Djava.awt.headless=true",
            )
        jdbc_url = f"jdbc:as400://{self.host}/{self.schema};prompt=false"
        return jaydebeapi.connect(
            "com.ibm.as400.access.AS400JDBCDriver",
            jdbc_url,
            [self.user, self.password],
            self.driver_path,
        )

    def ping(self):
        """Returns True if the AS/400 connection is successful."""
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("VALUES 1")
            cursor.fetchone()
        except Exception as e:
            logger.error(f"AS/400 connection failed: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        logger.info("AS/400 database connection is active.")
        return True

    def construct_query(self, query, preview, rows):
        if preview:
            query = query.strip().rstrip(";")
            query = f"SELECT * FROM ({query}) AS t FETCH FIRST {rows} ROWS ONLY"
        return query

    def extract_data_batch(self, table_name: str, offset: int = 0, limit: int = 100):
        query = (
            f'SELECT * FROM {self.schema}."{table_name}" '
            f"ORDER BY 1 "
            f"OFFSET {offset} ROWS "
            f"FETCH FIRST {limit} ROWS ONLY"
        )
        logger.info(f"Fetching batch: table={table_name}, offset={offset}, limit={limit}")
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            column_names = [str(col[0]) for col in cursor.description]
            rows = cursor.fetchall()
            return [
                {col: safe_convert_to_string(row[i]) for i, col in enumerate(column_names)}
                for row in rows
            ]
        except Exception as e:
            logger.error(f"Error extracting batch from {table_name}: {e}")
            return []
        finally:
            cursor.close()
            conn.close()

    def fetch_batch(self, cursor, table_name, offset: int, limit: int = 100):
        try:
            query = (
                f'SELECT * FROM {self.schema}."{table_name}" '
                f"ORDER BY 1 "
                f"OFFSET {offset} ROWS "
                f"FETCH FIRST {limit} ROWS ONLY"
            )
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error fetching batch from {table_name}: {e}")
            return []

    def stream_batch(self, cursor, table_name: str, batch_size: int = 10_000):
        """
        Full-sync streaming for DB2 AS/400.
        Uses a single SELECT and fetchmany to avoid OFFSET degradation.
        """
        try:
            logger.info(f"Start streaming AS/400 table {table_name} with batch_size={batch_size}")
            cursor.execute(f'SELECT * FROM {self.schema}."{table_name}"')

            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield rows

            logger.info(f"Finished streaming AS/400 table {table_name}")
        except Exception as exc:
            logger.error(f"Error streaming batch from AS/400 table {table_name}: {exc}")
            return

    def get_connection_tables(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT TABLE_NAME FROM QSYS2.SYSTABLES "
                "WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'T' "
                "ORDER BY TABLE_NAME",
                (self.schema,)
            )
            tables = [str(row[0]).strip() for row in cursor.fetchall()]
            return tables
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return []
        finally:
            cursor.close()
            conn.close()

    def get_connection_columns(self, table_name):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT COLUMN_NAME, DATA_TYPE "
                "FROM QSYS2.SYSCOLUMNS "
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                "ORDER BY ORDINAL_POSITION",
                (self.schema, table_name)
            )
            rows = cursor.fetchall()
            columns = [
                {
                    "name": str(row[0]).strip(),
                    "type": cast_db2_as400_to_typescript_types(str(row[1]).strip()),
                }
                for row in rows
            ]
            return columns
        except Exception as e:
            logger.error(f"Error getting columns: {e}")
            return []
        finally:
            cursor.close()
            conn.close()

    def count_table_rows(self, table_name: str) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f'SELECT COUNT(*) FROM {self.schema}."{table_name}"')
            result = cursor.fetchone()
            return int(result[0]) if result else 0
        except Exception as e:
            logger.error(f"Error getting table total rows: {e}")
            return 0
        finally:
            cursor.close()
            conn.close()

    def get_min_max_date(self, table_name: str, column_name: str):
        """
        Returns (min_value, max_value) for a DATE/TIMESTAMP column in a DB2 AS/400 table.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            sql = (
                f'SELECT MIN("{column_name}"), MAX("{column_name}") '
                f'FROM {self.schema}."{table_name}" '
                f'WHERE "{column_name}" IS NOT NULL'
            )
            logger.info(f"Getting min/max for {table_name}.{column_name}")
            cursor.execute(sql)
            row = cursor.fetchone()
            return (row[0], row[1]) if row else (None, None)
        finally:
            cursor.close()
            conn.close()

    def get_database_schema(self) -> Dict[str, Dict]:
        try:
            logger.debug("Getting database schema for DB2 AS/400 using JayDeBeApi")
            tables = {}
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute(
                "SELECT TABLE_NAME FROM QSYS2.SYSTABLES "
                "WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'T' "
                "ORDER BY TABLE_NAME",
                (self.schema,)
            )
            table_rows = cursor.fetchall()
            logger.debug(f"Found {len(table_rows)} tables")

            for table_row in table_rows:
                tname = str(table_row[0]).strip()
                try:
                    cursor.execute(
                        "SELECT COLUMN_NAME, DATA_TYPE, LENGTH "
                        "FROM QSYS2.SYSCOLUMNS "
                        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                        "ORDER BY ORDINAL_POSITION",
                        (self.schema, tname)
                    )
                    cols = cursor.fetchall()
                    column_list = [
                        {
                            "name": str(c[0]).strip(),
                            "type": cast_db2_as400_to_typescript_types(str(c[1]).strip()),
                            "length": int(c[2]) if c[2] is not None else 0,
                        }
                        for c in cols
                    ]
                    tables[tname] = {
                        "name": tname,
                        "owner": self.schema,
                        "columns": column_list,
                    }
                    logger.debug(f"Added table {tname} with {len(cols)} columns")
                except Exception as col_err:
                    logger.error(f"Error getting columns for table {tname}: {col_err}")
                    continue

            logger.debug(f"Retrieved schema for {len(tables)} tables")
            if len(tables) == 0:
                logger.warning("No tables found in the schema")
            return tables

        except Exception as e:
            logger.error(f"Error getting database schema: {e}")
            raise ValueError(f"Failed to retrieve database schema: {e}")
        finally:
            cursor.close()
            conn.close()

    def extract_table_schema(self, table_name):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            # 1. Get basic column info
            cursor.execute(
                "SELECT ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE, LENGTH, "
                "IS_NULLABLE, COLUMN_DEFAULT "
                "FROM QSYS2.SYSCOLUMNS "
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                "ORDER BY ORDINAL_POSITION",
                (self.schema, table_name)
            )
            rows = cursor.fetchall()

            columns = [
                {
                    "position": int(row[0]) if row[0] is not None else 0,
                    "name": str(row[1]).strip() if row[1] else row[1],
                    "type": cast_db2_as400_to_postgresql_type(str(row[2]).strip() if row[2] else ""),
                    "length": int(row[3]) if row[3] is not None else 0,
                    "nullable": str(row[4]).strip() if row[4] else "YES",
                    "default": str(row[5]) if row[5] is not None else None,
                    "primary_key": "NO",
                    "foreign_key": "NO",
                    "is_index": "NO",
                }
                for row in rows
            ]

            col_lookup = {col["name"]: col for col in columns}

            # 2. Get primary key columns
            try:
                cursor.execute(
                    "SELECT kc.COLUMN_NAME "
                    "FROM QSYS2.SYSKEYCST kc "
                    "JOIN QSYS2.SYSCST cst "
                    "  ON kc.CONSTRAINT_SCHEMA = cst.CONSTRAINT_SCHEMA "
                    "  AND kc.CONSTRAINT_NAME = cst.CONSTRAINT_NAME "
                    "WHERE cst.CONSTRAINT_TYPE = 'PRIMARY KEY' "
                    "  AND cst.TABLE_SCHEMA = ? AND cst.TABLE_NAME = ?",
                    (self.schema, table_name)
                )
                for row in cursor.fetchall():
                    col_name = str(row[0]).strip()
                    if col_name in col_lookup:
                        col_lookup[col_name]["primary_key"] = "YES"
            except Exception:
                logger.debug(f"Could not fetch PK info for {table_name}")

            # 3. Get foreign key columns
            try:
                cursor.execute(
                    "SELECT kc.COLUMN_NAME "
                    "FROM QSYS2.SYSKEYCST kc "
                    "JOIN QSYS2.SYSCST cst "
                    "  ON kc.CONSTRAINT_SCHEMA = cst.CONSTRAINT_SCHEMA "
                    "  AND kc.CONSTRAINT_NAME = cst.CONSTRAINT_NAME "
                    "WHERE cst.CONSTRAINT_TYPE = 'FOREIGN KEY' "
                    "  AND cst.TABLE_SCHEMA = ? AND cst.TABLE_NAME = ?",
                    (self.schema, table_name)
                )
                for row in cursor.fetchall():
                    col_name = str(row[0]).strip()
                    if col_name in col_lookup:
                        col_lookup[col_name]["foreign_key"] = "YES"
            except Exception:
                logger.debug(f"Could not fetch FK info for {table_name}")

            # 4. Get indexed columns
            try:
                cursor.execute(
                    "SELECT sk.COLUMN_NAME "
                    "FROM QSYS2.SYSKEYS sk "
                    "JOIN QSYS2.SYSINDEXES si "
                    "  ON sk.INDEX_SCHEMA = si.INDEX_SCHEMA "
                    "  AND sk.INDEX_NAME = si.INDEX_NAME "
                    "WHERE si.TABLE_SCHEMA = ? AND si.TABLE_NAME = ?",
                    (self.schema, table_name)
                )
                for row in cursor.fetchall():
                    col_name = str(row[0]).strip()
                    if col_name in col_lookup:
                        col_lookup[col_name]["is_index"] = "YES"
            except Exception:
                logger.debug(f"Could not fetch index info for {table_name}")

            return columns

        except Exception as e:
            logger.error(f"Error extracting schema for {table_name}: {e}")
            return []
        finally:
            cursor.close()
            conn.close()

    def fetch_deltas(
        self,
        cursor,
        primary_keys: List[str],
        log_table: str,
        since_ts: datetime,
        batch_size: int = 10_000,
    ) -> Iterator[Dict[str, Any]]:
        pk_cols = ", ".join(f'"{pk}"' for pk in primary_keys)
        order_by_final = ", ".join(f'"{pk}"' for pk in primary_keys)

        sql = f"""
            SELECT *
            FROM (
                SELECT t.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY {pk_cols}
                        ORDER BY "Date_operation" DESC
                    ) AS rn
                FROM {self.schema}."{log_table}" t
                WHERE "Date_operation" > ?
            ) AS ranked
            WHERE rn = 1
            ORDER BY {order_by_final}
            OFFSET ? ROWS
            FETCH FIRST ? ROWS ONLY
        """

        offset = 0
        while True:
            cursor.execute(sql, (since_ts, offset, batch_size))
            rows = cursor.fetchall()
            if not rows:
                break

            col_names = [str(desc[0]) for desc in cursor.description]
            for row in rows:
                yield {k: safe_convert_to_string(v) for k, v in zip(col_names, row)}

            offset += batch_size

    def truncate_table(self, table_name: str) -> bool:
        """
        Remove all data from the specified AS/400 table.
        Attempts TRUNCATE TABLE first; falls back to DELETE FROM
        since some AS/400 physical files do not support TRUNCATE.
        """
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            try:
                cursor.execute(f'TRUNCATE TABLE {self.schema}."{table_name}" IMMEDIATE')
                conn.commit()
                logger.info(f"Successfully truncated table: {self.schema}.{table_name}")
                return True
            except Exception:
                conn.rollback()
                logger.warning(
                    f"TRUNCATE not supported for {self.schema}.{table_name}, "
                    f"falling back to DELETE FROM"
                )
                cursor.execute(f'DELETE FROM {self.schema}."{table_name}"')
                conn.commit()
                logger.info(f"Successfully deleted all rows from: {self.schema}.{table_name}")
                return True
        except Exception as e:
            logger.error(f"Failed to truncate/delete table {self.schema}.{table_name}: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
