import ast
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock


def load_stream_batch():
    # Exercise the real method without requiring a native ODBC driver in unit tests.
    path = Path(__file__).parents[1] / "cmr_connectors_lib/database_connectors/informix_connector.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "InformixConnector")
    method = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "stream_batch")
    namespace = {
        "pyodbc": SimpleNamespace(Cursor=object), "logger": Mock(),
        "time": SimpleNamespace(sleep=Mock()),
        "is_retryable_connection_error": lambda exc: isinstance(exc, ConnectionError),
    }
    exec(compile(ast.Module(body=[method], type_ignores=[]), str(path), "exec"), namespace)
    return namespace["stream_batch"]


class InformixStreamBatchTests(unittest.TestCase):
    def setUp(self):
        self.stream = load_stream_batch()
        self.cursor = Mock()
        self.connection = Mock()
        self.connection.cursor.return_value = self.cursor
        def cleanup(cursor, connection):
            for resource in (cursor, connection):
                if resource is not None:
                    resource.close()

        self.connector = SimpleNamespace(
            _open_connection=Mock(return_value=self.connection),
            _safe_cleanup=cleanup,
            _log_retry=Mock(), _retry_delay=lambda attempt: 0,
            max_connection_retries=1,
        )

    def test_full_sync_keeps_existing_positional_call(self):
        self.cursor.fetchmany.side_effect = [[(1,), (2,)], [(3,)], []]
        self.assertEqual(list(self.stream(self.connector, None, "suiv_ech", 2)),
                         [[(1,), (2,)], [(3,)]])
        self.assertEqual(self.cursor.execute.call_count, 1)
        self.assertEqual(self.cursor.execute.call_args.args[0], "SELECT * FROM suiv_ech")
        self.assertTrue(all(call.args == (2,) for call in self.cursor.fetchmany.call_args_list))
        self.connection.close.assert_called_once()
        self.cursor.close.assert_called_once()

    def test_custom_query_binds_parameters_and_streams_one_result_set(self):
        query = "SELECT * FROM suiv_ech WHERE num_mandat = ?"
        self.cursor.fetchmany.side_effect = [[("102",)], []]
        result = list(self.stream(self.connector, None, "suiv_ech", 1,
                                  query=query, params=("102",)))
        self.assertEqual(result, [[("102",)]])
        self.cursor.execute.assert_called_once_with(query, ("102",))
        self.connection.close.assert_called_once()

    def test_fetch_failure_after_first_batch_raises_without_replaying(self):
        self.cursor.fetchmany.side_effect = [[(1,)], ConnectionError("lost connection")]
        stream = self.stream(self.connector, None, "suiv_ech", 1)
        self.assertEqual(next(stream), [(1,)])
        with self.assertRaisesRegex(ConnectionError, "lost connection"):
            next(stream)
        self.connector._open_connection.assert_called_once()
        self.connection.close.assert_called_once()
        self.cursor.close.assert_called_once()

    def test_retry_before_first_batch_reexecutes_same_query_and_parameters(self):
        replacement_cursor = Mock()
        replacement_cursor.fetchmany.side_effect = [[(102,)], []]
        replacement_connection = Mock()
        replacement_connection.cursor.return_value = replacement_cursor
        self.cursor.execute.side_effect = ConnectionError("connection lost before first row")
        self.connector._open_connection.side_effect = [self.connection, replacement_connection]
        query = "SELECT * FROM suiv_ech WHERE num_mandat = ?"
        self.assertEqual(list(self.stream(self.connector, None, "suiv_ech", 1,
                                         query=query, params=(102,))), [[(102,)]])
        self.cursor.execute.assert_called_once_with(query, (102,))
        replacement_cursor.execute.assert_called_once_with(query, (102,))
        self.connection.close.assert_called_once()
        replacement_connection.close.assert_called_once()

    def test_query_error_propagates_instead_of_appearing_empty(self):
        self.cursor.execute.side_effect = ValueError("invalid query")
        with self.assertRaisesRegex(ValueError, "invalid query"):
            list(self.stream(self.connector, None, "suiv_ech", 1))
        self.connection.close.assert_called_once()

    def test_consumer_closing_stream_releases_source_resources(self):
        self.cursor.fetchmany.return_value = [(1,)]
        stream = self.stream(self.connector, None, "suiv_ech", 1)
        next(stream)
        stream.close()
        self.connection.close.assert_called_once()
        self.cursor.close.assert_called_once()

    def test_cursor_creation_failure_closes_connection(self):
        self.connection.cursor.side_effect = RuntimeError("cursor creation failed")
        with self.assertRaisesRegex(RuntimeError, "cursor creation failed"):
            list(self.stream(self.connector, None, "suiv_ech", 1))
        self.connection.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
