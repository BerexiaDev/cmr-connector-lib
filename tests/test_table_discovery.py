"""A failed catalog query must never look like a successful empty database."""
import unittest
from unittest.mock import MagicMock, Mock

from loguru import logger
from cmr_connectors_lib.connectors_factory import ConnectorFactory


class TableDiscoveryTests(unittest.TestCase):
    def connector(self, kind):
        connector = ConnectorFactory().create_connector(kind, {
            "host": "db.test", "port": 1521, "database": "cmr",
            "user": "deepkube", "password": "test-password", "schema": "administrateur",
            "protocol": "onsoctcp", "locale": "en_US.utf8",
        })
        cursor = MagicMock()
        connection = MagicMock()
        connection.cursor.return_value = cursor
        connector._open_connection = Mock(return_value=connection)
        connector.get_connection = Mock(return_value=connection)
        connector._retry_delay = lambda _: 0
        return connector, cursor, connection

    def test_query_errors_propagate_and_close_resources(self):
        for kind in ("oracle", "postgres", "sqlserver", "informix"):
            with self.subTest(kind=kind):
                connector, cursor, connection = self.connector(kind)
                failure = RuntimeError("permission denied")
                cursor.execute.side_effect = failure
                with self.assertRaises(RuntimeError) as caught:
                    connector.get_connection_tables()
                self.assertIs(caught.exception, failure)
                self.assertTrue(cursor.close.called)
                self.assertTrue(connection.close.called)

    def test_empty_catalog_remains_an_empty_list(self):
        for kind in ("oracle", "postgres", "sqlserver", "informix"):
            with self.subTest(kind=kind):
                connector, cursor, connection = self.connector(kind)
                cursor.fetchall.return_value = []
                self.assertEqual(connector.get_connection_tables(), [])
                self.assertTrue(cursor.close.called)
                self.assertTrue(connection.close.called)

    def test_cursor_creation_failure_closes_connection(self):
        for kind in ("oracle", "postgres", "sqlserver", "informix"):
            with self.subTest(kind=kind):
                connector, cursor, connection = self.connector(kind)
                connection.cursor.side_effect = RuntimeError("cursor creation failed")
                with self.assertRaises(RuntimeError):
                    connector.get_connection_tables()
                self.assertTrue(connection.close.called)

    def test_oracle_retries_transport_failure_then_returns_tables(self):
        connector, cursor, connection = self.connector("oracle")
        cursor.execute.side_effect = [RuntimeError("ORA-03113: connection lost"), None]
        cursor.fetchall.return_value = [("CLIENTS",)]
        self.assertEqual(connector.get_connection_tables(), ["CLIENTS"])
        self.assertEqual(connector._open_connection.call_count, 2)

    def test_retry_logs_source_context_and_redacts_password(self):
        connector, cursor, connection = self.connector("oracle")
        cursor.execute.side_effect = [RuntimeError("ORA-03113: test-password"), None]
        cursor.fetchall.return_value = []
        logs = []
        sink = logger.add(logs.append, format="{message}", diagnose=True)
        try:
            self.assertEqual(connector.get_connection_tables(), [])
        finally:
            logger.remove(sink)
        text = "\n".join(str(message) for message in logs)
        for value in ("ORA-03113", "db.test", "cmr", "deepkube", "ADMINISTRATEUR"):
            self.assertIn(value, text)
        self.assertNotIn("test-password", text)

    def test_ping_failure_keeps_false_contract_and_logs_safe_traceback(self):
        for kind in ("oracle", "postgres", "sqlserver", "informix"):
            with self.subTest(kind=kind):
                connector, cursor, connection = self.connector(kind)
                connector._open_connection.side_effect = RuntimeError("authentication rejected: test-password")
                logs = []
                sink = logger.add(logs.append, format="{message}", diagnose=True)
                try:
                    self.assertFalse(connector.ping())
                finally:
                    logger.remove(sink)
                text = "\n".join(str(message) for message in logs)
                self.assertIn("Traceback", text)
                self.assertIn("db.test", text)
                self.assertNotIn("test-password", text)


if __name__ == "__main__":
    unittest.main()
