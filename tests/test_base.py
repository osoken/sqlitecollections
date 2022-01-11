import os
import re
import sqlite3
import sys
import uuid
from collections.abc import Hashable
from typing import Any
from unittest import TestCase
from unittest.mock import MagicMock, patch

if sys.version_info > (3, 9):
    from collections.abc import Callable
else:
    from typing import Callable

from sqlitecollections import base


class SqlAwareMagicMock(MagicMock):
    @classmethod
    def normalize_sql(cls, s: str) -> str:
        """remove newlines and consecutive whitespace-like characters."""
        if isinstance(s, str):
            return re.sub(r"\s+", " ", re.sub(r"\n", " ", s))
        return s

    def assert_called_once_with(_mock_self, *args: Any, **kwargs: Any) -> None:
        return super().assert_called_once_with(
            *[_mock_self.normalize_sql(s) for s in args],
            **{k: _mock_self.normalize_sql(v) for k, v in kwargs.items()},
        )


class SqlTestCase(TestCase):
    import sqlite3

    def assert_sql_result_equals(self, conn: sqlite3.Connection, sql: str, expected: Any) -> None:
        cur = conn.cursor()
        cur.execute(sql)
        return self.assertEqual(list(cur), expected)

    def get_fixture(self, conn: sqlite3.Connection, *fixture_names: str) -> None:
        wd = os.path.dirname(os.path.abspath(__file__))
        cursor = conn.cursor()
        for fixture_name in fixture_names:
            with open(os.path.join(wd, "fixtures", fixture_name), "r") as fin:
                cursor.executescript(fin.read())
        conn.commit()

    def assert_metadata_state_equals(self, conn: sqlite3.Connection, expected: Any) -> None:
        self.assert_sql_result_equals(conn, "SELECT table_name, schema_version, container_type FROM metadata", expected)


class ConcreteSqliteCollectionDatabaseDriver(base._SqliteCollectionBaseDatabaseDriver):
    @classmethod
    def do_create_table(
        cls, table_name: str, container_type_nam: str, schema_version: str, cur: sqlite3.Cursor
    ) -> None:
        cur.execute(f"CREATE TABLE {table_name} (idx INTEGER AUTO INCREMENT, value BLOB)")

    @classmethod
    def add(cls, table_name: str, value: bytes, cur: sqlite3.Cursor) -> None:
        cur.execute(f"INSERT INTO {table_name} (value) VALUES (?)", (value,))


class ConcreteSqliteCollectionClass(base.SqliteCollectionBase[Any]):
    _driver_class = ConcreteSqliteCollectionDatabaseDriver

    @property
    def schema_version(self) -> str:
        return "test_0"

    def _rebuild_check_with_first_element(self) -> bool:
        return False

    def _do_rebuild(self) -> None:
        ...

    def add(self, value: bytes) -> None:
        cur = self.connection.cursor()
        self._driver_class.add(self.table_name, value, cur)
        self.connection.commit()


class SqliteCollectionsBaseTestCase(SqlTestCase):
    @patch(
        "sqlitecollections.base.uuid4",
        return_value=uuid.UUID("4da95358-64e7-40e7-b888-31e14e1c1d09"),
    )
    @patch("sqlitecollections.base.loads")
    @patch("sqlitecollections.base.dumps")
    @patch("sqlitecollections.base.sqlite3.connect")
    @patch("sqlitecollections.base.NamedTemporaryFile")
    def test_init_with_no_args(
        self,
        NamedTemporaryFile: MagicMock,
        sqlite3_connect: MagicMock,
        dumps: MagicMock,
        loads: MagicMock,
        uuid4: MagicMock,
    ) -> None:
        memory_db = sqlite3.Connection(":memory:")
        NamedTemporaryFile.return_value.name = "tempfilename"
        sqlite3_connect.return_value = memory_db
        sut = ConcreteSqliteCollectionClass()
        sqlite3_connect.assert_called_once_with("tempfilename")
        self.assertEqual(sut.connection, memory_db)
        self.assertEqual(sut.serializer, dumps)
        self.assertEqual(sut.deserializer, loads)
        self.assertEqual(sut.persist, True)
        self.assertEqual(
            sut.table_name,
            "ConcreteSqliteCollectionClass_4da9535864e740e7b88831e14e1c1d09",
        )
        self.assert_metadata_state_equals(
            memory_db,
            [
                (
                    "ConcreteSqliteCollectionClass_4da9535864e740e7b88831e14e1c1d09",
                    "test_0",
                    "ConcreteSqliteCollectionClass",
                )
            ],
        )
        self.assert_sql_result_equals(
            memory_db,
            "SELECT 1 FROM ConcreteSqliteCollectionClass_4da9535864e740e7b88831e14e1c1d09",
            [],
        )
        self.assertFalse(hasattr(sut, "rebuild_strategy"))

    @patch("sqlitecollections.base.sqlite3.connect")
    def test_init_with_args(self, sqlite3_connect: MagicMock) -> None:
        memory_db = sqlite3.Connection(":memory:")
        sqlite3_connect.return_value = memory_db
        serializer = MagicMock(spec=Callable[[Any], bytes])
        deserializer = MagicMock(spec=Callable[[bytes], Any])
        persist = False
        rebuild_strategy = base.RebuildStrategy.SKIP
        sut = ConcreteSqliteCollectionClass(
            connection="connection",
            table_name="tablename",
            serializer=serializer,
            deserializer=deserializer,
            persist=persist,
            rebuild_strategy=rebuild_strategy,
        )
        sqlite3_connect.assert_called_once_with("connection")
        self.assertEqual(sut.connection, memory_db)
        self.assertEqual(sut.serializer, serializer)
        self.assertEqual(sut.deserializer, deserializer)
        self.assertEqual(
            sut.table_name,
            "tablename",
        )
        self.assertEqual(sut.persist, persist)
        self.assert_metadata_state_equals(
            memory_db,
            [
                (
                    "tablename",
                    "test_0",
                    "ConcreteSqliteCollectionClass",
                )
            ],
        )
        self.assert_sql_result_equals(
            memory_db,
            "SELECT 1 FROM tablename",
            [],
        )

    @patch(
        "sqlitecollections.base.uuid4",
        return_value=uuid.UUID("4da95358-64e7-40e7-b888-31e14e1c1d09"),
    )
    def test_init_with_connection(self, uuid4: MagicMock) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = ConcreteSqliteCollectionClass(connection=memory_db)
        self.assert_metadata_state_equals(
            memory_db,
            [
                (
                    "ConcreteSqliteCollectionClass_4da9535864e740e7b88831e14e1c1d09",
                    "test_0",
                    "ConcreteSqliteCollectionClass",
                )
            ],
        )
        self.assert_sql_result_equals(
            memory_db,
            "SELECT 1 FROM ConcreteSqliteCollectionClass_4da9535864e740e7b88831e14e1c1d09",
            [],
        )

    def test_change_table_name(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = ConcreteSqliteCollectionClass(connection=memory_db, table_name="before")
        sut.add(b"before_content")
        self.assert_metadata_state_equals(
            memory_db,
            [
                (
                    "before",
                    "test_0",
                    "ConcreteSqliteCollectionClass",
                )
            ],
        )
        self.assert_sql_result_equals(
            memory_db,
            "SELECT value FROM before",
            [(b"before_content",)],
        )
        sut.table_name = "after"
        self.assert_metadata_state_equals(
            memory_db,
            [
                (
                    "after",
                    "test_0",
                    "ConcreteSqliteCollectionClass",
                )
            ],
        )
        self.assert_sql_result_equals(
            memory_db,
            "SELECT value FROM after",
            [(b"before_content",)],
        )
        with self.assertRaises(sqlite3.OperationalError):
            self.assert_sql_result_equals(
                memory_db,
                "SELECT value FROM before",
                [(b"before_content",)],
            )

    def test_error_on_change_table_name_which_already_exists(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = ConcreteSqliteCollectionClass(connection=memory_db, table_name="before")
        sut2 = ConcreteSqliteCollectionClass(connection=memory_db, table_name="after")
        self.assert_metadata_state_equals(
            memory_db,
            [
                (
                    "before",
                    "test_0",
                    "ConcreteSqliteCollectionClass",
                ),
                (
                    "after",
                    "test_0",
                    "ConcreteSqliteCollectionClass",
                ),
            ],
        )
        with self.assertRaises(ValueError):
            sut.table_name = "after"
        self.assert_metadata_state_equals(
            memory_db,
            [
                (
                    "before",
                    "test_0",
                    "ConcreteSqliteCollectionClass",
                ),
                (
                    "after",
                    "test_0",
                    "ConcreteSqliteCollectionClass",
                ),
            ],
        )

    def test_init_same_container_twice(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = ConcreteSqliteCollectionClass(connection=memory_db, table_name="items")
        sut2 = ConcreteSqliteCollectionClass(connection=memory_db, table_name="items")
        self.assert_metadata_state_equals(
            memory_db,
            [
                (
                    "items",
                    "test_0",
                    "ConcreteSqliteCollectionClass",
                )
            ],
        )
        self.assert_sql_result_equals(
            memory_db,
            "SELECT 1 FROM items",
            [],
        )

    def test_init_different_containers(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = ConcreteSqliteCollectionClass(connection=memory_db, table_name="items1")
        sut2 = ConcreteSqliteCollectionClass(connection=memory_db, table_name="items2")
        self.assert_metadata_state_equals(
            memory_db,
            [
                (
                    "items1",
                    "test_0",
                    "ConcreteSqliteCollectionClass",
                ),
                (
                    "items2",
                    "test_0",
                    "ConcreteSqliteCollectionClass",
                ),
            ],
        )
        self.assert_sql_result_equals(
            memory_db,
            "SELECT 1 FROM items1",
            [],
        )
        self.assert_sql_result_equals(
            memory_db,
            "SELECT 1 FROM items2",
            [],
        )

    def test_destruct_container(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = ConcreteSqliteCollectionClass(connection=memory_db, table_name="items1", persist=False)
        sut2 = ConcreteSqliteCollectionClass(connection=memory_db, table_name="items2", persist=True)
        self.assert_metadata_state_equals(
            memory_db,
            [
                (
                    "items1",
                    "test_0",
                    "ConcreteSqliteCollectionClass",
                ),
                (
                    "items2",
                    "test_0",
                    "ConcreteSqliteCollectionClass",
                ),
            ],
        )
        self.assert_sql_result_equals(
            memory_db,
            "SELECT 1 FROM items1",
            [],
        )
        self.assert_sql_result_equals(
            memory_db,
            "SELECT 1 FROM items2",
            [],
        )
        del sut
        del sut2
        self.assert_sql_result_equals(
            memory_db,
            "SELECT table_name, schema_version, container_type FROM metadata",
            [
                (
                    "items2",
                    "test_0",
                    "ConcreteSqliteCollectionClass",
                ),
            ],
        )
        with self.assertRaises(sqlite3.OperationalError):
            cur = memory_db.cursor()
            cur.execute("SELECT 1 FROM items1")
        self.assert_sql_result_equals(
            memory_db,
            "SELECT 1 FROM items2",
            [],
        )

    @patch.object(ConcreteSqliteCollectionClass, "_rebuild_check_with_first_element", return_value=True)
    @patch.object(ConcreteSqliteCollectionClass, "_do_rebuild", return_value=None)
    def test_set_persist(self, _do_rebuild: MagicMock, _rebuild_check_with_first_element: MagicMock) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = ConcreteSqliteCollectionClass(connection=memory_db, table_name="items1", persist=True)
        self.assertTrue(sut.persist)
        sut.set_persist(False)
        self.assertFalse(sut.persist)
        sut = ConcreteSqliteCollectionClass(connection=memory_db, table_name="items2", persist=False)
        self.assertFalse(sut.persist)
        sut.set_persist(True)
        self.assertTrue(sut.persist)

    @patch.object(ConcreteSqliteCollectionClass, "_rebuild_check_with_first_element", return_value=True)
    @patch.object(ConcreteSqliteCollectionClass, "_do_rebuild", return_value=None)
    def test_rebuild_check_with_first_element(
        self, _do_rebuild: MagicMock, _rebuild_check_with_first_element: MagicMock
    ) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = ConcreteSqliteCollectionClass(
            connection=memory_db, table_name="items1", rebuild_strategy=base.RebuildStrategy.CHECK_WITH_FIRST_ELEMENT
        )
        _rebuild_check_with_first_element.assert_called_once_with()
        _do_rebuild.assert_called_once_with()

    @patch.object(ConcreteSqliteCollectionClass, "_rebuild_check_with_first_element", return_value=True)
    @patch.object(ConcreteSqliteCollectionClass, "_do_rebuild", return_value=None)
    def test_rebuild_always(self, _do_rebuild: MagicMock, _rebuild_check_with_first_element: MagicMock) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = ConcreteSqliteCollectionClass(
            connection=memory_db, table_name="items1", rebuild_strategy=base.RebuildStrategy.ALWAYS
        )
        _rebuild_check_with_first_element.assert_not_called()
        _do_rebuild.assert_called_once_with()

    @patch.object(ConcreteSqliteCollectionClass, "_rebuild_check_with_first_element", return_value=True)
    @patch.object(ConcreteSqliteCollectionClass, "_do_rebuild", return_value=None)
    def test_rebuild_skip(self, _do_rebuild: MagicMock, _rebuild_check_with_first_element: MagicMock) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = ConcreteSqliteCollectionClass(
            connection=memory_db, table_name="items1", rebuild_strategy=base.RebuildStrategy.SKIP
        )
        _rebuild_check_with_first_element.assert_not_called()
        _do_rebuild.assert_not_called()
