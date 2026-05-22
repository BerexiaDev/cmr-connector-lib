#!/usr/bin/python
# -*- coding: utf-8 -*-

from abc import abstractmethod
from datetime import datetime
import time
from typing import Iterator, Dict, Any, Callable

import pyodbc
from pyodbc import Cursor
from loguru import logger


RETRYABLE_CONNECTION_ERROR_MARKERS = (
    "ssl error",
    "invalid alert",
    "communication link failure",
    "connection reset",
    "connection closed",
    "connection refused",
    "socket",
    "timeout",
    "08s01",
    "08001",
    "datadirect",
)

NON_RETRYABLE_CONNECTION_ERROR_MARKERS = (
    "syntax error",
    "incorrect syntax",
    "invalid syntax",
    "42000",
    "42s02",
    "42s22",
    "42p01",
    "42703",
    "column not found",
    "table not found",
    "undefined column",
    "undefined table",
    "invalid column name",
    "invalid object name",
    "permission denied",
    "not authorized",
    "42501",
    "conversion failed",
    "data conversion",
    "invalid character value for cast specification",
    "duplicate key",
    "unique constraint",
    "primary key violation",
    "constraint violation",
    "23000",
    "23503",
    "23505",
    "23514",
    "22001",
    "22003",
    "22007",
    "22008",
    "22018",
    "22026",
)


def _build_error_message(error: Exception) -> str:
    parts = [str(error)]
    for arg in getattr(error, "args", ()):
        text = str(arg)
        if text and text not in parts:
            parts.append(text)
    return " | ".join(parts).lower()


def is_retryable_connection_error(error: Exception) -> bool:
    message = _build_error_message(error)
    if any(marker in message for marker in NON_RETRYABLE_CONNECTION_ERROR_MARKERS):
        return False
    return any(marker in message for marker in RETRYABLE_CONNECTION_ERROR_MARKERS)


class SqlConnector():
    max_connection_retries = 3
    retry_backoff_seconds = 2

    def __init__(self, host, user, password, port, database):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.database = database
        self.driver = None

    
    @abstractmethod
    def get_connection(self):
        """Returns a connection object from the driver."""

    def _open_connection(self):
        return self.get_connection()

    def _connector_name(self) -> str:
        return self.__class__.__name__

    def _retry_delay(self, retry_number: int) -> int:
        return retry_number * self.retry_backoff_seconds

    def _log_retry(self, operation_name: str, retry_number: int, error: Exception):
        logger.warning(
            f"{self._connector_name()} retry {retry_number}/{self.max_connection_retries} "
            f"for {operation_name}: {error}"
        )

    @staticmethod
    def _safe_close(resource):
        if resource is None:
            return
        try:
            resource.close()
        except Exception:
            pass

    def _safe_cleanup(self, cursor=None, connection=None):
        cursor_connection = getattr(cursor, "connection", None)
        self._safe_close(cursor)

        closed = set()
        for resource in (connection, cursor_connection):
            if resource is None:
                continue
            resource_id = id(resource)
            if resource_id in closed:
                continue
            self._safe_close(resource)
            closed.add(resource_id)

    def _connect_with_retry(self, connect_callable: Callable[[], Any], operation_name: str = "connect"):
        retry_number = 0
        while True:
            try:
                return connect_callable()
            except Exception as error:
                if not is_retryable_connection_error(error) or retry_number >= self.max_connection_retries:
                    raise

                retry_number += 1
                self._log_retry(operation_name, retry_number, error)
                time.sleep(self._retry_delay(retry_number))

    def _run_operation_with_retry(self, operation_name: str, operation: Callable[[Any, Any], Any]):
        retry_number = 0
        while True:
            connection = None
            cursor = None
            try:
                connection = self._open_connection()
                cursor = connection.cursor()
                return operation(connection, cursor)
            except Exception as error:
                self._safe_cleanup(cursor, connection)
                if not is_retryable_connection_error(error) or retry_number >= self.max_connection_retries:
                    raise

                retry_number += 1
                self._log_retry(operation_name, retry_number, error)
                time.sleep(self._retry_delay(retry_number))
            finally:
                self._safe_cleanup(cursor, connection)

    def _run_existing_cursor_operation_with_retry(
        self,
        operation_name: str,
        cursor: Cursor,
        operation: Callable[[Cursor], Any],
    ):
        active_cursor = cursor
        active_connection = getattr(cursor, "connection", None)
        owns_retry_resources = False
        retry_number = 0

        try:
            while True:
                try:
                    return operation(active_cursor)
                except Exception as error:
                    if not is_retryable_connection_error(error) or retry_number >= self.max_connection_retries:
                        raise

                    self._safe_cleanup(active_cursor, active_connection)
                    retry_number += 1
                    self._log_retry(operation_name, retry_number, error)
                    time.sleep(self._retry_delay(retry_number))

                    active_connection = self._open_connection()
                    active_cursor = active_connection.cursor()
                    owns_retry_resources = True
        finally:
            if owns_retry_resources:
                self._safe_cleanup(active_cursor, active_connection)

    def ping(self):
        """Returns True if the connection is successful, False otherwise."""
        try:
            def _ping(_, cursor):
                cursor.execute("SELECT 1")
                cursor.fetchone()
                return True

            self._run_operation_with_retry("ping", _ping)
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False

        logger.info("Database connection is active.")
        return True
    
    @abstractmethod
    def get_connection_tables(self):
        """
        Returns a list of all table names in the given database.
        """

    @abstractmethod
    def get_connection_columns(self, table_name):
        """Returns a list of dictionaries with column names and types for the given table."""


    def get_database_schema(self):
        pass

    @abstractmethod
    def extract_data_batch(self, table_name: str, offset: int = 0, limit: int = 100):
        """
           Extracts a batch of rows from a table using SKIP/FIRST.
           Defaults to the first 100 rows if offset/limit are not provided.
        """

    @abstractmethod
    def fetch_batch(self, cursor: Cursor, table_name, offset: int, limit: int = 100):
        """
          Fetch up to `limit` rows from `table`, skipping the first `offset` rows.

        Args:
            table_name (str):       Name of the Informix table.
            offset (int):      Number of rows to skip.
            limit (int):       Maximum rows to return.
            cursor (Cursor):       An active database cursor. Must remain open; do not close it inside this method.

        Returns:
            list of tuple:     The fetched rows, empty if none remain.
        """

    @abstractmethod
    def extract_table_schema(self, table_name: str):
        """
           Gather column-level details from database including:
           - column name, type, nullability, default
           - primary key, foreign key, and index flags
        """

    @abstractmethod
    def fetch_deltas( self, cursor, primary_key: str, log_table: str, since_ts: datetime, batch_size: int = 10_000) -> Iterator[Dict[str, Any]]:
        """
        Pull delta rows from <base_table>_log newer than since_ts.
        Uses LIMIT/OFFSET for pagination.
        """
        
    @abstractmethod
    def truncate_table(self, table_name: str) -> bool:
        """
        Remove all data from the specified table while keeping its structure
        """
