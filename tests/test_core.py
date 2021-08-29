import os
import pickle
import re
import sqlite3
import sys
import uuid
from collections.abc import Hashable
from typing import Any
from unittest import TestCase
from unittest.mock import MagicMock, patch

if sys.version_info > (3, 9):
    from collections.abc import Callable, ItemsView, Iterator, KeysView, ValuesView
else:
    from typing import ItemsView, KeysView, ValuesView, Iterator, Callable

from sqlitecollections import core


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


class SqliteCollectionsBaseTestCase(SqlTestCase):
    class ConcreteSqliteCollectionClass(core.SqliteCollectionBase[Any]):
        @property
        def schema_version(self) -> str:
            return "test_0"

        def _do_create_table(self, commit: bool = False) -> None:
            cur = self.connection.cursor()
            cur.execute(f"CREATE TABLE {self.table_name} (idx INTEGER AUTO INCREMENT, value BLOB)")

        def _rebuild_check_with_first_element(self) -> bool:
            return False

        def _do_rebuild(self, commit: bool = False) -> None:
            ...

    @patch(
        "sqlitecollections.core.uuid4",
        return_value=uuid.UUID("4da95358-64e7-40e7-b888-31e14e1c1d09"),
    )
    @patch("sqlitecollections.core.loads")
    @patch("sqlitecollections.core.dumps")
    @patch("sqlitecollections.core.sqlite3.connect")
    @patch("sqlitecollections.core.NamedTemporaryFile")
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
        sut = self.ConcreteSqliteCollectionClass()
        sqlite3_connect.assert_called_once_with("tempfilename")
        self.assertEqual(sut.connection, memory_db)
        self.assertEqual(sut.serializer, dumps)
        self.assertEqual(sut.deserializer, loads)
        self.assertEqual(sut.destruct_table_on_delete, False)
        self.assertEqual(sut.rebuild_strategy, core.RebuildStrategy.CHECK_WITH_FIRST_ELEMENT)
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

    @patch("sqlitecollections.core.sqlite3.connect")
    def test_init_with_args(self, sqlite3_connect: MagicMock) -> None:
        memory_db = sqlite3.Connection(":memory:")
        sqlite3_connect.return_value = memory_db
        serializer = MagicMock(spec=Callable[[Any], bytes])
        deserializer = MagicMock(spec=Callable[[bytes], Any])
        destruct_table_on_delete = True
        rebuild_strategy = core.RebuildStrategy.SKIP
        sut = self.ConcreteSqliteCollectionClass(
            connection="connection",
            table_name="tablename",
            serializer=serializer,
            deserializer=deserializer,
            destruct_table_on_delete=destruct_table_on_delete,
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
        self.assertEqual(sut.destruct_table_on_delete, destruct_table_on_delete)
        self.assertEqual(sut.rebuild_strategy, rebuild_strategy)
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
        "sqlitecollections.core.uuid4",
        return_value=uuid.UUID("4da95358-64e7-40e7-b888-31e14e1c1d09"),
    )
    def test_init_with_connection(self, uuid4: MagicMock) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = self.ConcreteSqliteCollectionClass(connection=memory_db)
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

    def test_init_same_container_twice(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = self.ConcreteSqliteCollectionClass(connection=memory_db, table_name="items")
        sut2 = self.ConcreteSqliteCollectionClass(connection=memory_db, table_name="items")
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
        sut = self.ConcreteSqliteCollectionClass(connection=memory_db, table_name="items1")
        sut2 = self.ConcreteSqliteCollectionClass(connection=memory_db, table_name="items2")
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
        sut = self.ConcreteSqliteCollectionClass(
            connection=memory_db, table_name="items1", destruct_table_on_delete=True
        )
        sut2 = self.ConcreteSqliteCollectionClass(
            connection=memory_db, table_name="items2", destruct_table_on_delete=False
        )
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
    def test_rebuild_check_with_first_element(
        self, _do_rebuild: MagicMock, _rebuild_check_with_first_element: MagicMock
    ) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = self.ConcreteSqliteCollectionClass(
            connection=memory_db, table_name="items1", rebuild_strategy=core.RebuildStrategy.CHECK_WITH_FIRST_ELEMENT
        )
        _rebuild_check_with_first_element.assert_called_once_with()
        _do_rebuild.assert_called_once_with(commit=True)

    @patch.object(ConcreteSqliteCollectionClass, "_rebuild_check_with_first_element", return_value=True)
    @patch.object(ConcreteSqliteCollectionClass, "_do_rebuild", return_value=None)
    def test_rebuild_always(self, _do_rebuild: MagicMock, _rebuild_check_with_first_element: MagicMock) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = self.ConcreteSqliteCollectionClass(
            connection=memory_db, table_name="items1", rebuild_strategy=core.RebuildStrategy.ALWAYS
        )
        _rebuild_check_with_first_element.assert_not_called()
        _do_rebuild.assert_called_once_with(commit=True)

    @patch.object(ConcreteSqliteCollectionClass, "_rebuild_check_with_first_element", return_value=True)
    @patch.object(ConcreteSqliteCollectionClass, "_do_rebuild", return_value=None)
    def test_rebuild_skip(self, _do_rebuild: MagicMock, _rebuild_check_with_first_element: MagicMock) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = self.ConcreteSqliteCollectionClass(
            connection=memory_db, table_name="items1", rebuild_strategy=core.RebuildStrategy.SKIP
        )
        _rebuild_check_with_first_element.assert_not_called()
        _do_rebuild.assert_not_called()


class DictTestCase(SqlTestCase):
    def assert_dict_state_equals(self, conn: sqlite3.Connection, expected: Any) -> None:
        return self.assert_sql_result_equals(
            conn,
            """
                SELECT serialized_key, serialized_value, item_order
                FROM items ORDER BY item_order
            """,
            expected,
        )

    @patch("sqlitecollections.core.Dict._initialize", return_value=None)
    @patch("sqlitecollections.core.SqliteCollectionBase.__init__", return_value=None)
    @patch("sqlitecollections.core.SqliteCollectionBase.__del__", return_value=None)
    def test_init(
        self, SqliteCollectionBase_del: MagicMock, SqliteCollectionBase_init: MagicMock, _initialize: MagicMock
    ) -> None:
        memory_db = sqlite3.connect(":memory:")
        table_name = "items"
        serializer = MagicMock(spec=Callable[[Any], bytes])
        deserializer = MagicMock(spec=Callable[[bytes], Any])
        key_serializer = MagicMock(spec=Callable[[Hashable], bytes])
        key_deserializer = MagicMock(spec=Callable[[bytes], Hashable])
        destruct_table_on_delete = True
        rebuild_strategy = core.RebuildStrategy.SKIP
        sut = core.Dict[Hashable, Any](
            connection=memory_db,
            table_name=table_name,
            serializer=serializer,
            deserializer=deserializer,
            key_serializer=key_serializer,
            key_deserializer=key_deserializer,
            destruct_table_on_delete=destruct_table_on_delete,
            rebuild_strategy=rebuild_strategy,
        )
        SqliteCollectionBase_init.assert_called_once_with(
            connection=memory_db,
            table_name=table_name,
            serializer=serializer,
            deserializer=deserializer,
            destruct_table_on_delete=destruct_table_on_delete,
            rebuild_strategy=rebuild_strategy,
            do_initialize=False,
        )
        self.assertEqual(sut.key_serializer, key_serializer)
        self.assertEqual(sut.key_deserializer, key_deserializer)

    def test_initialize(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = core.Dict[Hashable, Any](connection=memory_db, table_name="items")
        self.assert_sql_result_equals(
            memory_db,
            "SELECT table_name, schema_version, container_type FROM metadata",
            [
                (
                    "items",
                    sut.schema_version,
                    sut.container_type_name,
                ),
            ],
        )
        self.assert_dict_state_equals(
            memory_db,
            [],
        )

    def test_rebuild(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict_base.sql", "dict_rebuild.sql")

        def serializer(x: str) -> bytes:
            return x.upper().encode("utf-8")

        def deserializer(x: bytes) -> str:
            return str(x)

        sut = core.Dict[str, str](
            connection=memory_db,
            table_name="items",
            serializer=serializer,
            deserializer=deserializer,
            rebuild_strategy=core.RebuildStrategy.ALWAYS,
        )
        self.assert_dict_state_equals(memory_db, [(b"A", b"B", 0)])

    def test_init_with_initial_data(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = core.Dict[Hashable, Any](
            connection=memory_db,
            table_name="items",
            data={"a": 1, "b": 2},
            a=4,
            c=None,
            d=[1, 2],
        )
        self.assert_dict_state_equals(
            memory_db,
            [
                (pickle.dumps("a"), pickle.dumps(4), 0),
                (pickle.dumps("b"), pickle.dumps(2), 1),
                (pickle.dumps("c"), pickle.dumps(None), 2),
                (pickle.dumps("d"), pickle.dumps([1, 2]), 3),
            ],
        )

    def test_getitem(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict_base.sql", "dict_getitem.sql")
        sut = core.Dict[Hashable, Any](connection=memory_db, table_name="items")
        expected1 = 4
        actual1 = sut["a"]
        self.assertEqual(actual1, expected1)
        expected2 = [1, 2]
        actual2 = sut["d"]
        self.assertEqual(actual2, expected2)
        with self.assertRaisesRegex(KeyError, "nonsuch"):
            _ = sut["nonsuch"]
        with self.assertRaisesRegex(TypeError, r"unhashable type:"):
            _ = sut[[0, 1]]  # type: ignore

    def test_delitem(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict_base.sql", "dict_delitem.sql")
        sut = core.Dict[Hashable, Any](connection=memory_db, table_name="items")
        del sut["b"]
        self.assert_dict_state_equals(
            memory_db,
            [
                (pickle.dumps("a"), pickle.dumps(4), 0),
            ],
        )
        with self.assertRaisesRegex(KeyError, "b"):
            del sut["b"]

        del sut["a"]
        self.assert_dict_state_equals(
            memory_db,
            [],
        )
        with self.assertRaisesRegex(TypeError, r"unhashable type:"):
            del sut[[0, 1]]  # type: ignore

    def test_setitem(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict_base.sql")
        sut = core.Dict[Hashable, Any](connection=memory_db, table_name="items")
        self.assert_dict_state_equals(
            memory_db,
            [],
        )
        sut["akey"] = {"a": "dict"}
        self.assert_dict_state_equals(
            memory_db,
            [(pickle.dumps("akey"), pickle.dumps({"a": "dict"}), 0)],
        )
        sut["anotherkey"] = ["a", "b"]
        self.assert_dict_state_equals(
            memory_db,
            [
                (pickle.dumps("akey"), pickle.dumps({"a": "dict"}), 0),
                (pickle.dumps("anotherkey"), pickle.dumps(["a", "b"]), 1),
            ],
        )
        sut["akey"] = None
        self.assert_dict_state_equals(
            memory_db,
            [
                (pickle.dumps("akey"), pickle.dumps(None), 0),
                (pickle.dumps("anotherkey"), pickle.dumps(["a", "b"]), 1),
            ],
        )
        with self.assertRaisesRegex(TypeError, r"unhashable type:"):
            sut[[0, 1]] = 0  # type: ignore

    def test_len(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict_base.sql")
        sut = core.Dict[Hashable, Any](connection=memory_db, table_name="items")
        expected = 0
        actual = len(sut)
        self.assertEqual(actual, expected)
        self.get_fixture(memory_db, "dict_len.sql")
        expected = 4
        actual = len(sut)
        self.assertEqual(actual, expected)

    def test_contains(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict_base.sql", "dict_contains.sql")
        sut = core.Dict[Hashable, Any](connection=memory_db, table_name="items")
        self.assertTrue("a" in sut)
        self.assertTrue(b"a" in sut)
        self.assertTrue(None in sut)
        self.assertTrue(0 in sut)
        self.assertFalse(100 in sut)
        self.assertTrue(((0, 1), "a") in sut)
        with self.assertRaisesRegex(TypeError, r"unhashable type:"):
            _ = [0, 1] in sut  # type: ignore

        self.assertFalse("a" not in sut)
        self.assertFalse(b"a" not in sut)
        self.assertFalse(None not in sut)
        self.assertFalse(0 not in sut)
        self.assertFalse(((0, 1), "a") not in sut)
        self.assertTrue(100 not in sut)
        with self.assertRaisesRegex(TypeError, r"unhashable type:"):
            _ = [0, 1] not in sut  # type: ignore

    def test_iter(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict_base.sql")
        sut = core.Dict[Hashable, Any](connection=memory_db, table_name="items")
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), [])
        self.assertEqual(list(actual), [])
        self.get_fixture(memory_db, "dict_iter.sql")
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), ["a", "b", "c", "d"])

    def test_clear(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict_base.sql")
        sut = core.Dict[Hashable, Any](connection=memory_db, table_name="items")
        self.assert_dict_state_equals(memory_db, [])
        sut.clear()
        self.assert_dict_state_equals(memory_db, [])
        self.get_fixture(memory_db, "dict_clear.sql")
        sut = core.Dict(connection=memory_db, table_name="items")
        self.assert_dict_state_equals(memory_db, [(pickle.dumps("a"), pickle.dumps(4), 0)])
        sut.clear()
        self.assert_dict_state_equals(memory_db, [])

    def test_get(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict_base.sql", "dict_get.sql")
        sut = core.Dict[Hashable, Any](connection=memory_db, table_name="items")
        expected1 = 4
        actual1 = sut.get("a")
        self.assertEqual(actual1, expected1)
        expected2 = [1, 2]
        actual2 = sut.get("d")
        self.assertEqual(actual2, expected2)
        expected3 = None
        actual3 = sut.get("nonsuch")
        self.assertEqual(actual3, expected3)
        expected4 = "default"
        actual4 = sut.get("nonsuch", "default")
        self.assertEqual(actual4, expected4)
        with self.assertRaisesRegex(TypeError, r"unhashable type:"):
            _ = sut.get([0, 1])  # type: ignore

    def test_items(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict_base.sql", "dict_items.sql")
        sut = core.Dict[Hashable, Any](connection=memory_db, table_name="items")
        actual = sut.items()
        self.assertIsInstance(actual, ItemsView)
        expected = [("a", 4), ("b", 2)]
        self.assertEqual(list(actual), expected)

    def test_keys(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict_base.sql", "dict_keys.sql")
        sut = core.Dict[Hashable, Any](connection=memory_db, table_name="items")
        actual = sut.keys()
        self.assertIsInstance(actual, KeysView)
        expected = ["a", "b"]
        self.assertEqual(list(actual), expected)

    def test_pop(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict_base.sql", "dict_pop.sql")
        sut = core.Dict[Hashable, Any](connection=memory_db, table_name="items")
        self.assert_dict_state_equals(
            memory_db,
            [
                (pickle.dumps("a"), pickle.dumps(4), 0),
                (pickle.dumps("b"), pickle.dumps(2), 1),
                (pickle.dumps("c"), pickle.dumps(None), 2),
                (pickle.dumps("d"), pickle.dumps([1, 2]), 3),
            ],
        )
        expected1 = 4
        actual1 = sut.pop("a")
        self.assertEqual(actual1, expected1)
        self.assert_dict_state_equals(
            memory_db,
            [
                (pickle.dumps("b"), pickle.dumps(2), 1),
                (pickle.dumps("c"), pickle.dumps(None), 2),
                (pickle.dumps("d"), pickle.dumps([1, 2]), 3),
            ],
        )
        expected2 = "default"
        actual2 = sut.pop("nonsuch", "default")
        self.assertEqual(actual2, expected2)
        self.assert_dict_state_equals(
            memory_db,
            [
                (pickle.dumps("b"), pickle.dumps(2), 1),
                (pickle.dumps("c"), pickle.dumps(None), 2),
                (pickle.dumps("d"), pickle.dumps([1, 2]), 3),
            ],
        )
        with self.assertRaisesRegex(KeyError, "nonsuch"):
            _ = sut.pop("nonsuch")
        self.assert_dict_state_equals(
            memory_db,
            [
                (pickle.dumps("b"), pickle.dumps(2), 1),
                (pickle.dumps("c"), pickle.dumps(None), 2),
                (pickle.dumps("d"), pickle.dumps([1, 2]), 3),
            ],
        )

    def test_popitem(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict_base.sql", "dict_popitem.sql")
        sut = core.Dict[Hashable, Any](connection=memory_db, table_name="items")
        self.assert_dict_state_equals(
            memory_db,
            [
                (pickle.dumps("a"), pickle.dumps(4), 0),
                (pickle.dumps("b"), pickle.dumps(2), 1),
            ],
        )
        if sys.version_info < (3, 7):
            expected = sorted([("b", 2), ("a", 4)])
            actual = sorted([sut.popitem(), sut.popitem()])
            self.assertEqual(actual, expected)
            self.assert_dict_state_equals(memory_db, [])

            with self.assertRaisesRegex(KeyError, r"'popitem\(\): dictionary is empty'"):
                _ = sut.popitem()
        else:
            expected = ("b", 2)
            actual = sut.popitem()
            self.assertEqual(actual, expected)
            self.assert_dict_state_equals(
                memory_db,
                [
                    (pickle.dumps("a"), pickle.dumps(4), 0),
                ],
            )

            expected = ("a", 4)
            actual = sut.popitem()
            self.assertEqual(actual, expected)
            self.assert_dict_state_equals(memory_db, [])

            with self.assertRaisesRegex(KeyError, r"'popitem\(\): dictionary is empty'"):
                _ = sut.popitem()

    def test_reversed(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict_base.sql", "dict_reversed.sql")
        sut = core.Dict[Hashable, Any](connection=memory_db, table_name="items")
        if sys.version_info < (3, 8):
            with self.assertRaisesRegex(TypeError, "'Dict' object is not reversible"):
                _ = reversed(sut)  # type: ignore
        else:
            actual = reversed(sut)
            self.assertIsInstance(actual, Iterator)
            expected = ["b", "a"]
            self.assertEqual(list(actual), expected)

    def test_setdefault(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict_base.sql", "dict_setdefault.sql")
        sut = core.Dict[Hashable, Any](connection=memory_db, table_name="items")
        self.assert_dict_state_equals(
            memory_db,
            [
                (pickle.dumps("a"), pickle.dumps(4), 0),
                (pickle.dumps("b"), pickle.dumps(2), 1),
            ],
        )
        expected1 = 4
        actual1 = sut.setdefault("a")
        self.assertEqual(actual1, expected1)
        self.assert_dict_state_equals(
            memory_db,
            [
                (pickle.dumps("a"), pickle.dumps(4), 0),
                (pickle.dumps("b"), pickle.dumps(2), 1),
            ],
        )
        expected2 = None
        actual2 = sut.setdefault("c")
        self.assertEqual(actual2, expected2)
        self.assert_dict_state_equals(
            memory_db,
            [
                (pickle.dumps("a"), pickle.dumps(4), 0),
                (pickle.dumps("b"), pickle.dumps(2), 1),
                (pickle.dumps("c"), pickle.dumps(None), 2),
            ],
        )
        expected3 = 2
        actual3 = sut.setdefault("b", "default")
        self.assertEqual(actual3, expected3)
        self.assert_dict_state_equals(
            memory_db,
            [
                (pickle.dumps("a"), pickle.dumps(4), 0),
                (pickle.dumps("b"), pickle.dumps(2), 1),
                (pickle.dumps("c"), pickle.dumps(None), 2),
            ],
        )
        expected4 = "default"
        actual4 = sut.setdefault("d", "default")
        self.assertEqual(actual4, expected4)
        self.assert_dict_state_equals(
            memory_db,
            [
                (pickle.dumps("a"), pickle.dumps(4), 0),
                (pickle.dumps("b"), pickle.dumps(2), 1),
                (pickle.dumps("c"), pickle.dumps(None), 2),
                (pickle.dumps("d"), pickle.dumps("default"), 3),
            ],
        )

    def test_update(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict_base.sql", "dict_update.sql")
        sut = core.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut.update({"a": 1, "e": 10})
        self.assert_dict_state_equals(
            memory_db,
            [
                (pickle.dumps("a"), pickle.dumps(1), 0),
                (pickle.dumps("b"), pickle.dumps(2), 1),
                (pickle.dumps("e"), pickle.dumps(10), 2),
            ],
        )

    def test_values(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict_base.sql", "dict_values.sql")
        sut = core.Dict[Hashable, Any](connection=memory_db, table_name="items")
        actual = sut.values()
        self.assertIsInstance(actual, ValuesView)
        expected = [4, 2]
        self.assertEqual(list(actual), expected)

    def test_or(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict_base.sql", "dict_or.sql")
        sut = core.Dict[Hashable, Any](connection=memory_db, table_name="items")
        if sys.version_info < (3, 9):
            with self.assertRaisesRegex(
                TypeError,
                r"unsupported operand type\(s\) for \|: 'Dict' and '[a-zA-Z]+'",
            ):
                sut | {"a": 1, "e": 10}  # type: ignore
        else:
            actual = sut | {"a": 1, "e": 10}
            self.assert_sql_result_equals(
                memory_db,
                f"SELECT serialized_key, serialized_value, item_order FROM {actual.table_name} ORDER BY item_order",
                [
                    (pickle.dumps("a"), pickle.dumps(1), 0),
                    (pickle.dumps("b"), pickle.dumps(2), 1),
                    (pickle.dumps("e"), pickle.dumps(10), 2),
                ],
            )

    def test_ior(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict_base.sql", "dict_ior.sql")
        sut = core.Dict[Hashable, Any](connection=memory_db, table_name="items")
        if sys.version_info < (3, 9):
            with self.assertRaisesRegex(
                TypeError,
                r"unsupported operand type\(s\) for \|=: 'Dict' and '[a-zA-Z]+'",
            ):
                sut |= {"a": 1, "e": 10}  # type: ignore
        else:
            sut |= {"a": 1, "e": 10}
            self.assert_dict_state_equals(
                memory_db,
                [
                    (pickle.dumps("a"), pickle.dumps(1), 0),
                    (pickle.dumps("b"), pickle.dumps(2), 1),
                    (pickle.dumps("e"), pickle.dumps(10), 2),
                ],
            )


class SetTestCase(SqlTestCase):
    def assert_sql_result_equals(self, conn: sqlite3.Connection, sql: str, expected: Any) -> None:
        cur = conn.cursor()
        cur.execute(sql)
        return self.assertEqual(sorted(cur), sorted(expected))

    def assert_db_state_equals(self, conn: sqlite3.Connection, expected: Any) -> None:
        return self.assert_sql_result_equals(
            conn,
            "SELECT serialized_value FROM items",
            expected,
        )

    def assert_items_table_only(self, conn: sqlite3.Connection) -> None:
        return self.assert_metadata_state_equals(conn, [("items", "0", "Set")])

    @patch("sqlitecollections.core.SqliteCollectionBase.__init__", return_value=None)
    @patch("sqlitecollections.core.SqliteCollectionBase.__del__", return_value=None)
    def test_init(self, SqliteCollectionBase_del: MagicMock, SqliteCollectionBase_init: MagicMock) -> None:
        memory_db = sqlite3.connect(":memory:")
        table_name = "items"
        serializer = MagicMock(spec=Callable[[Hashable], bytes])
        deserializer = MagicMock(spec=Callable[[bytes], Hashable])
        destruct_table_on_delete = True
        rebuild_strategy = core.RebuildStrategy.SKIP
        sut = core.Set[Hashable](
            connection=memory_db,
            table_name=table_name,
            serializer=serializer,
            deserializer=deserializer,
            destruct_table_on_delete=destruct_table_on_delete,
            rebuild_strategy=rebuild_strategy,
        )
        SqliteCollectionBase_init.assert_called_once_with(
            connection=memory_db,
            table_name=table_name,
            serializer=serializer,
            deserializer=deserializer,
            destruct_table_on_delete=destruct_table_on_delete,
            rebuild_strategy=rebuild_strategy,
            do_initialize=True,
        )

    def test_initialize(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        self.assert_items_table_only(memory_db)
        self.assert_db_state_equals(
            memory_db,
            [],
        )

    def test_rebuild(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_rebuild.sql")

        def serializer(x: str) -> bytes:
            return x.upper().encode("utf-8")

        def deserializer(x: bytes) -> str:
            return str(x)

        sut = core.Set[str](
            connection=memory_db,
            table_name="items",
            serializer=serializer,
            deserializer=deserializer,
            rebuild_strategy=core.RebuildStrategy.ALWAYS,
        )
        self.assert_db_state_equals(
            memory_db,
            [
                (b"A",),
                (b"B",),
                (b"C",),
                (b"D",),
                (b"E",),
                (b"F",),
                (b"G",),
                (b"H",),
            ],
        )

    def test_init_with_initial_data(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = core.Set[Hashable](
            connection=memory_db,
            table_name="items",
            data=["a", "b", "a", "a", "aa", b"bb"],
        )
        self.assert_db_state_equals(
            memory_db,
            [
                (pickle.dumps("a"),),
                (pickle.dumps("b"),),
                (pickle.dumps("aa"),),
                (pickle.dumps(b"bb"),),
            ],
        )

    def test_len(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        expected = 0
        actual = len(sut)
        self.assertEqual(actual, expected)
        self.get_fixture(memory_db, "set_len.sql")
        expected = 2
        actual = len(sut)
        self.assertEqual(actual, expected)

    def test_contains(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_contains.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        self.assertTrue("a" in sut)
        self.assertTrue(b"a" in sut)
        self.assertTrue(None in sut)
        self.assertTrue(0 in sut)
        self.assertFalse(100 in sut)
        self.assertTrue(((0, 1), "a") in sut)
        with self.assertRaisesRegex(TypeError, r"unhashable type:"):
            _ = [0, 1] in sut  # type: ignore

        self.assertFalse("a" not in sut)
        self.assertFalse(b"a" not in sut)
        self.assertFalse(None not in sut)
        self.assertFalse(0 not in sut)
        self.assertFalse(((0, 1), "a") not in sut)
        self.assertTrue(100 not in sut)
        with self.assertRaisesRegex(TypeError, r"unhashable type:"):
            _ = [0, 1] not in sut  # type: ignore

    def test_iter(self) -> None:
        from typing import Sequence

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        actual = iter(sut)
        expected: Sequence[Hashable] = []
        self.assertEqual(list(actual), expected)
        self.get_fixture(memory_db, "set_iter.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        actual = iter(sut)
        expected = sorted(["a", "b", "c"])
        self.assertEqual(sorted(list(actual)), expected)
        self.assert_items_table_only(memory_db)

    def test_isdisjoint(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_isdisjoint.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        self.assertFalse(sut.isdisjoint({"a", "b"}))
        self.assertTrue(sut.isdisjoint({1, 2, 3}))
        self.assertTrue(sut.isdisjoint({}))
        self.assertFalse(sut.isdisjoint(sut))
        self.assert_items_table_only(memory_db)

    def test_issubset(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_issubset.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        self.assertFalse(sut.issubset({"a"}))
        self.assertTrue(sut.issubset({"a", "b", "c", "d"}))
        self.assertTrue(sut.issubset(sut))
        self.assert_items_table_only(memory_db)

    def test_intersection_update(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_intersection_update.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        sut.intersection_update([1, 2, 3])
        self.assert_db_state_equals(memory_db, [])
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_intersection_update.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        sut.intersection_update(["a", "b"], ["b"])
        self.assert_db_state_equals(memory_db, [(pickle.dumps("b"),)])
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_intersection_update.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        sut.intersection_update(sut)
        self.assert_db_state_equals(memory_db, [(pickle.dumps("a"),), (pickle.dumps("b"),), (pickle.dumps("c"),)])
        self.assert_items_table_only(memory_db)

    def test_intersection(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_intersection.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        actual = sut.intersection([1, 2, 3])
        self.assert_sql_result_equals(memory_db, f"SELECT serialized_value FROM {actual.table_name}", [])
        actual = sut.intersection(["a", "b"], ["b"])
        self.assert_sql_result_equals(
            memory_db, f"SELECT serialized_value FROM {actual.table_name}", [(pickle.dumps("b"),)]
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_le(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_le.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        self.assertFalse(sut <= {"a"})
        self.assertTrue(sut <= {"a", "b", "c", "d"})
        self.assertTrue(sut <= sut)
        self.assert_items_table_only(memory_db)

    def test_lt(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_lt.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        self.assertFalse(sut < {"a"})
        self.assertTrue(sut < {"a", "b", "c", "d"})
        self.assertFalse(sut < sut)
        self.assert_items_table_only(memory_db)

    def test_issuperset(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_issuperset.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        self.assertTrue(sut.issuperset({"a"}))
        self.assertFalse(sut.issuperset({"a", "b", "c", "d"}))
        self.assertFalse(sut.issuperset([1]))
        self.assertTrue(sut.issuperset(sut))
        self.assert_items_table_only(memory_db)

    def test_union(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_union.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        actual = sut.union([1, 2, 3])
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (pickle.dumps("a"),),
                (pickle.dumps("b"),),
                (pickle.dumps("c"),),
                (pickle.dumps(1),),
                (pickle.dumps(2),),
                (pickle.dumps(3),),
            ],
        )

        actual = sut.union(["a", "b"], ["b"])
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (pickle.dumps("a"),),
                (pickle.dumps("b"),),
                (pickle.dumps("c"),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_union_update(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_union_update.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        sut.union_update([1, 2, 3])
        self.assert_db_state_equals(
            memory_db,
            [
                (pickle.dumps("a"),),
                (pickle.dumps("b"),),
                (pickle.dumps("c"),),
                (pickle.dumps(1),),
                (pickle.dumps(2),),
                (pickle.dumps(3),),
            ],
        )
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_union_update.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        sut.union_update(["a", "b"], ["b"])
        self.assert_db_state_equals(memory_db, [(pickle.dumps("a"),), (pickle.dumps("b"),), (pickle.dumps("c"),)])
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_union_update.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        sut.union_update(sut)
        self.assert_db_state_equals(memory_db, [(pickle.dumps("a"),), (pickle.dumps("b"),), (pickle.dumps("c"),)])
        self.assert_items_table_only(memory_db)

    def test_ge(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_ge.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        self.assertTrue(sut >= {"a"})
        self.assertFalse(sut >= {"a", "b", "c", "d"})
        self.assertTrue(sut >= sut)
        self.assert_items_table_only(memory_db)

    def test_gt(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_gt.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        self.assertTrue(sut > {"a"})
        self.assertFalse(sut > {"a", "b", "c", "d"})
        self.assertFalse(sut > sut)
        self.assert_items_table_only(memory_db)

    def test_or(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_or.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        actual = sut | {1, 2, 3}

        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (pickle.dumps("a"),),
                (pickle.dumps("b"),),
                (pickle.dumps("c"),),
                (pickle.dumps(1),),
                (pickle.dumps(2),),
                (pickle.dumps(3),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual = sut | {"a", "b"} | {"b"}
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (pickle.dumps("a"),),
                (pickle.dumps("b"),),
                (pickle.dumps("c"),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_and(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_and.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        actual = sut & {1, 2, 3}

        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [],
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual = sut & {"a", "b"} & {"b"}
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (pickle.dumps("b"),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_difference(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_difference.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        actual = sut.difference([1, 2, 3])

        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (pickle.dumps("a"),),
                (pickle.dumps("b"),),
                (pickle.dumps("c"),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual = sut.difference(["a", "b"], {"b"})
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (pickle.dumps("c"),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_copy(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_copy.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        actual = sut.copy()

        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (pickle.dumps("a"),),
                (pickle.dumps("b"),),
                (pickle.dumps("c"),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_difference_update(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_difference_update.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        sut.difference_update([1, 2, 3])
        self.assert_db_state_equals(
            memory_db,
            [
                (pickle.dumps("a"),),
                (pickle.dumps("b"),),
                (pickle.dumps("c"),),
            ],
        )
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_difference_update.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        sut.difference_update(["a", "b"], ["b"])
        self.assert_db_state_equals(memory_db, [(pickle.dumps("c"),)])
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set_base.sql", "set_difference_update.sql")
        sut = core.Set[Hashable](connection=memory_db, table_name="items")
        sut.difference_update(sut)
        self.assert_db_state_equals(memory_db, [])
        self.assert_items_table_only(memory_db)
