import os
import re
import sqlite3
import sys
import uuid
import warnings
from collections.abc import Hashable
from types import GeneratorType
from typing import Any, Optional, Union, cast
from unittest import TestCase
from unittest.mock import MagicMock, call, patch

if sys.version_info >= (3, 9):
    from collections.abc import Callable, Iterable, Sequence
else:
    from typing import Callable, Iterable, Sequence

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
        return self.assertEqual(self.get_sql_result(conn, sql), expected)

    def get_sql_result(self, conn: sqlite3.Connection, sql: str) -> Sequence[Any]:
        cur = conn.cursor()
        cur.execute(sql)
        return list(cur)

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
    if sys.version_info >= (3, 9):

        @classmethod
        @property
        def schema_version(cls) -> str:
            return "test_0"

    else:
        schema_version = "test_0"

    @classmethod
    def do_create_table(cls, table_name: str, container_type_nam: str, cur: sqlite3.Cursor) -> None:
        cur.execute(f"CREATE TABLE {table_name} (idx INTEGER AUTO INCREMENT, value BLOB)")

    @classmethod
    def add(cls, table_name: str, value: bytes, cur: sqlite3.Cursor) -> None:
        cur.execute(f"INSERT INTO {table_name} (value) VALUES (?)", (value,))


class ConcreteSqliteCollectionClass(base.SqliteCollectionBase[Any]):
    _driver_class = ConcreteSqliteCollectionDatabaseDriver

    def __init__(
        self,
        __data: Optional[Any] = None,
        connection: Optional[Union[str, sqlite3.Connection]] = None,
        table_name: Optional[str] = None,
        serializer: Optional[Callable[[Any], bytes]] = None,
        deserializer: Optional[Callable[[bytes], Any]] = None,
        persist: bool = True,
        pickling_strategy: base.PicklingStrategy = base.PicklingStrategy.whole_table,
    ) -> None:
        if (
            isinstance(__data, ConcreteSqliteCollectionClass)
            and __data.connection == connection
            and __data.serializer == serializer
            and __data.deserializer == deserializer
        ):
            super(ConcreteSqliteCollectionClass, self).__init__(
                connection=connection,
                table_name=table_name,
                serializer=serializer,
                deserializer=deserializer,
                persist=persist,
                pickling_strategy=pickling_strategy,
                reference_table_name=__data.table_name,
            )
        else:
            super(ConcreteSqliteCollectionClass, self).__init__(
                connection=connection,
                table_name=table_name,
                serializer=serializer,
                deserializer=deserializer,
                persist=persist,
                pickling_strategy=pickling_strategy,
            )

    def add(self, value: bytes) -> None:
        cur = self.connection.cursor()
        self._driver_class.add(self.table_name, value, cur)
        self.connection.commit()


class MetadataItemTestCase(TestCase):
    def test_metadata_item_has_table_name_schema_version_and_container_type_ro_attrs(self) -> None:
        table_name = "the_table_name"
        container_type = "SomeContainer"
        schema_version = "0.1"
        sut = base.MetadataItem(table_name=table_name, container_type=container_type, schema_version=schema_version)
        self.assertEqual(sut.table_name, table_name)
        self.assertEqual(sut.container_type, container_type)
        self.assertEqual(sut.schema_version, schema_version)
        expected_regex = "can't set attribute"
        if sys.version_info > (3, 11):
            expected_regex = "property '[a-z_]+' of 'MetadataItem' object has no setter"
        with self.assertRaisesRegex(AttributeError, expected_regex):
            sut.table_name = "new_table_name"  # type: ignore
        with self.assertRaisesRegex(AttributeError, expected_regex):
            sut.container_type = "AnotherContainer"  # type: ignore
        with self.assertRaisesRegex(AttributeError, expected_regex):
            sut.schema_version = "0.1.1"  # type: ignore

    def test_metadata_item_is_hashable(self) -> None:
        sut = base.MetadataItem(table_name="aa", container_type="SomeType", schema_version="-2")
        self.assertIsInstance(sut, Hashable)


class MetadataReaderTestCase(TestCase):
    def test_len(self) -> None:
        memory_db = sqlite3.Connection(":memory:")
        sut = base.MetadataReader(connection=memory_db)
        self.assertEqual(len(sut), 0)
        ConcreteSqliteCollectionClass(connection=memory_db, table_name="item")
        self.assertEqual(len(sut), 1)
        ConcreteSqliteCollectionClass(connection=memory_db, table_name="item2")
        self.assertEqual(len(sut), 2)
        ConcreteSqliteCollectionClass(connection=memory_db, table_name="item2")
        self.assertEqual(len(sut), 2)

    def test_contains_by_metadata(self) -> None:
        memory_db = sqlite3.Connection(":memory:")
        sut = base.MetadataReader(connection=memory_db)
        dummy_metadata = base.MetadataItem(table_name="dummy", schema_version="0.1", container_type="dummy")
        self.assertFalse(dummy_metadata in sut)
        item = ConcreteSqliteCollectionClass(connection=memory_db, table_name="item")
        item_metadata = base.MetadataItem(
            table_name="item",
            schema_version="test_0",
            container_type=item.container_type_name,
        )
        item2_metadata = base.MetadataItem(
            table_name="item2",
            schema_version="test_0",
            container_type=item.container_type_name,
        )
        self.assertTrue(item_metadata in sut)
        self.assertFalse(item2_metadata in sut)

    def test_iter(self) -> None:
        memory_db = sqlite3.Connection(":memory:")
        sut = base.MetadataReader(connection=memory_db)
        actual = iter(sut)
        self.assertIsInstance(actual, GeneratorType)
        actual_set = set(actual)
        self.assertEqual(actual_set, set())
        item = ConcreteSqliteCollectionClass(connection=memory_db, table_name="item")
        item_metadata = base.MetadataItem(
            table_name="item", container_type=item.container_type_name, schema_version="test_0"
        )
        ConcreteSqliteCollectionClass(connection=memory_db, table_name="item2")
        item2 = ConcreteSqliteCollectionClass(connection=memory_db, table_name="item2")
        item2_metadata = base.MetadataItem(
            table_name="item2",
            container_type=item2.container_type_name,
            schema_version="test_0",
        )
        actual = iter(sut)
        self.assertIsInstance(actual, GeneratorType)
        actual_set = set(actual)
        self.assertEqual(actual_set, {item_metadata, item2_metadata})


class SqliteCollectionsBaseTestCase(SqlTestCase):
    def test_default_serializer(self) -> None:
        expected = b'\x80\x03X\x03\x00\x00\x00123q\x00.'
        actual = ConcreteSqliteCollectionClass._default_serializer("123")
        self.assertEqual(actual, expected)

    def test_default_deserializer(self) -> None:
        expected = "123"
        actual = ConcreteSqliteCollectionClass._default_deserializer(b'\x80\x03X\x03\x00\x00\x00123q\x00.')
        self.assertEqual(actual, expected)

    @patch("sqlitecollections.base.dumps")
    def test_protocol_is_specified_on_calling_default_serializer(self, dumps: MagicMock) -> None:
        actual = ConcreteSqliteCollectionClass._default_serializer("x")
        expected = dumps.return_value
        dumps.assert_called_once_with("x", protocol=3)
        self.assertEqual(actual, expected)

    @patch("sqlitecollections.base.sanitize_table_name", return_value="ConcreteSqliteCollectionClass_4da95_sanitized")
    @patch("sqlitecollections.base.create_random_name", return_value="ConcreteSqliteCollectionClass_4da95")
    @patch("sqlitecollections.base.SqliteCollectionBase._default_serializer")
    @patch("sqlitecollections.base.SqliteCollectionBase._default_deserializer")
    @patch("sqlitecollections.base.tidy_connection")
    def test_init_with_no_args(
        self,
        tidy_connection: MagicMock,
        default_deserializer: MagicMock,
        default_serializer: MagicMock,
        create_random_name: MagicMock,
        sanitize_table_name: MagicMock,
    ) -> None:
        memory_db = sqlite3.Connection(":memory:")
        tidy_connection.return_value = memory_db
        sut = ConcreteSqliteCollectionClass()
        tidy_connection.assert_called_once_with(None)
        self.assertEqual(sut.connection, memory_db)
        self.assertEqual(sut.serializer, default_serializer)
        self.assertEqual(sut.deserializer, default_deserializer)
        self.assertEqual(sut.persist, True)
        self.assertEqual(
            sut.table_name,
            "ConcreteSqliteCollectionClass_4da95_sanitized",
        )
        self.assert_metadata_state_equals(
            memory_db,
            [
                (
                    "ConcreteSqliteCollectionClass_4da95_sanitized",
                    "test_0",
                    "ConcreteSqliteCollectionClass",
                )
            ],
        )
        self.assert_sql_result_equals(
            memory_db,
            "SELECT 1 FROM ConcreteSqliteCollectionClass_4da95_sanitized",
            [],
        )
        self.assertEqual(sut.pickling_strategy, base.PicklingStrategy.whole_table)
        create_random_name.assert_called_once_with(sut.container_type_name)
        sanitize_table_name.assert_called_once_with(create_random_name.return_value, sut.container_type_name)

    @patch("sqlitecollections.base.warnings.warn")
    @patch("sqlitecollections.base.sanitize_table_name", return_value="sanitized_tablename")
    @patch("sqlitecollections.base.tidy_connection")
    def test_init_with_args(self, tidy_connection: MagicMock, sanize_table_name: MagicMock, warn: MagicMock) -> None:
        memory_db = sqlite3.Connection(":memory:")
        tidy_connection.return_value = memory_db
        serializer = MagicMock(spec=Callable[[Any], bytes])
        deserializer = MagicMock(spec=Callable[[bytes], Any])
        persist = False
        pickling_strategy = base.PicklingStrategy.only_file_name
        sut = ConcreteSqliteCollectionClass(
            connection="connection",
            table_name="tablename",
            serializer=serializer,
            deserializer=deserializer,
            persist=persist,
            pickling_strategy=pickling_strategy,
        )
        tidy_connection.assert_called_once_with("connection")
        self.assertEqual(sut.connection, memory_db)
        self.assertEqual(sut.serializer, serializer)
        self.assertEqual(sut.deserializer, deserializer)
        self.assertEqual(
            sut.table_name,
            "sanitized_tablename",
        )
        self.assertEqual(sut.persist, persist)
        self.assert_metadata_state_equals(
            memory_db,
            [
                (
                    "sanitized_tablename",
                    "test_0",
                    "ConcreteSqliteCollectionClass",
                )
            ],
        )
        self.assert_sql_result_equals(
            memory_db,
            "SELECT 1 FROM sanitized_tablename",
            [],
        )
        self.assertEqual(sut.pickling_strategy, pickling_strategy)
        sanize_table_name.assert_called_once_with("tablename", sut.container_type_name)

        warn.assert_not_called()

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

    def test_init_with_reference_table(self) -> None:
        import random

        memory_db = sqlite3.connect(":memory:")
        sut = ConcreteSqliteCollectionClass(connection=memory_db, table_name="items")
        for _ in range(100):
            sut.add(str(random.randint(0, 1000)).encode("utf-8"))
        actual = ConcreteSqliteCollectionClass(
            sut,
            connection=sut.connection,
            table_name="copied",
            serializer=sut.serializer,
            deserializer=sut.deserializer,
        )
        self.assertEqual(
            self.get_sql_result(memory_db, "SELECT idx, value FROM items ORDER BY idx"),
            self.get_sql_result(memory_db, "SELECT idx, value FROM copied ORDER BY idx"),
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

    def test_table_alteration_doesnt_occur_on_same_table_name(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = ConcreteSqliteCollectionClass(connection=memory_db, table_name="before")
        self.assert_metadata_state_equals(
            memory_db,
            [
                (
                    "before",
                    "test_0",
                    "ConcreteSqliteCollectionClass",
                ),
            ],
        )
        sut.table_name = "before"
        self.assert_metadata_state_equals(
            memory_db,
            [
                (
                    "before",
                    "test_0",
                    "ConcreteSqliteCollectionClass",
                ),
            ],
        )

    @patch("sqlitecollections.base.logger")
    def test_warning_on_table_name_sanitization(self, logger: MagicMock) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = ConcreteSqliteCollectionClass(connection=memory_db, table_name="tblnm; DELETE * FROM metadata")
        self.assert_metadata_state_equals(
            memory_db,
            [
                (
                    "tblnmDELETEFROMmetadata",
                    "test_0",
                    "ConcreteSqliteCollectionClass",
                ),
            ],
        )
        logger.warning.assert_called_once_with(
            "The table name is changed to tblnmDELETEFROMmetadata due to illegal characters"
        )

    @patch("sqlitecollections.base.sanitize_table_name", side_effect=["before_sanitized", "after_sanitized"])
    def test_sanitize_table_name_is_called_on_table_name_change(self, sanitize_table_name: MagicMock) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = ConcreteSqliteCollectionClass(connection=memory_db, table_name="before")
        sut.table_name = "after"
        sanitize_table_name.assert_has_calls(
            [call("before", sut.container_type_name), call("after", sut.container_type_name)]
        )

    @patch("sqlitecollections.base.logger")
    def test_warning_on_sanitized_table_name_on_change(self, logger: MagicMock) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = ConcreteSqliteCollectionClass(connection=memory_db, table_name="before")
        self.assert_metadata_state_equals(
            memory_db,
            [
                (
                    "before",
                    "test_0",
                    "ConcreteSqliteCollectionClass",
                ),
            ],
        )
        sut.table_name = "after; DELETE * FROM metadata"
        logger.warning.assert_called_once_with(
            "The table name is changed to afterDELETEFROMmetadata due to illegal characters"
        )
        self.assert_metadata_state_equals(
            memory_db,
            [
                (
                    "afterDELETEFROMmetadata",
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

    def test_set_persist(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = ConcreteSqliteCollectionClass(connection=memory_db, table_name="items1", persist=True)
        self.assertTrue(sut.persist)
        sut.set_persist(False)
        self.assertFalse(sut.persist)
        sut = ConcreteSqliteCollectionClass(connection=memory_db, table_name="items2", persist=False)
        self.assertFalse(sut.persist)
        sut.set_persist(True)
        self.assertTrue(sut.persist)


class SanitizeTableNameTestCase(TestCase):
    def test_sanitize_table_name(self) -> None:
        expected = "_qwerty01234abc"
        actual = base.sanitize_table_name("~!@#$%^&*()_+-=qwerty{}|[]\\;:'\"<>?,./01234abc", "sc")
        self.assertEqual(actual, expected)

    def test_sanitize_table_name_accepts_prefix_but_ignore_if_table_name_is_valid(self) -> None:
        expected = "abc"
        actual = base.sanitize_table_name("abc", "list")
        self.assertEqual(actual, expected)

    @patch("sqlitecollections.base.create_random_name", return_value="generated_random_name")
    def test_sanitize_table_name_generate_random_name_when_table_name_is_empty(
        self, create_random_name: MagicMock
    ) -> None:
        expected = "generated_random_name"
        actual = base.sanitize_table_name("", "prefix")
        self.assertEqual(actual, expected)
        create_random_name.assert_called_once_with("prefix")

    @patch("sqlitecollections.base.create_random_name", return_value="generated_random_name")
    def test_sanitize_table_name_generate_random_name_when_sanitized_table_name_is_empty(
        self, create_random_name: MagicMock
    ) -> None:
        expected = "generated_random_name"
        actual = base.sanitize_table_name(";;;;;", "prefix")
        self.assertEqual(actual, expected)
        create_random_name.assert_called_once_with("prefix")

    def test_add_prefix_when_table_name_with_leading_digit(self) -> None:
        expected = "prefix_123"
        actual = base.sanitize_table_name("+123", "prefix")
        self.assertEqual(actual, expected)


class IsHashableTestCase(TestCase):
    def test_is_hashable(self) -> None:
        self.assertTrue(base.is_hashable((1, 2)))
        self.assertTrue(base.is_hashable(1))
        self.assertTrue(base.is_hashable(None))
        self.assertTrue(base.is_hashable(True))
        self.assertTrue(base.is_hashable(False))
        self.assertTrue(base.is_hashable(b"123"))
        self.assertTrue(base.is_hashable("123"))
        self.assertTrue(base.is_hashable(frozenset([1, 2, 3])))
        self.assertFalse(base.is_hashable([1, 2]))
        self.assertFalse(base.is_hashable({1, 2, 3}))
        self.assertFalse(base.is_hashable({"a": 1}))


class CreateTemporaryDbFileTestCase(TestCase):
    @patch("sqlitecollections.base.NamedTemporaryFile")
    def test_create_temporary_db_file(self, NamedTemporaryFile: MagicMock) -> None:
        expected = NamedTemporaryFile.return_value
        actual = base.create_temporary_db_file()
        self.assertEqual(actual, expected)
        NamedTemporaryFile.assert_called_once_with(suffix=".db", prefix="sc_")


class CreateTempfileConnectionTestCase(TestCase):
    @patch("sqlitecollections.base.sqlite3.connect")
    @patch("sqlitecollections.base.create_temporary_db_file")
    def test_create_tempfile_connection(self, create_temporary_db_file: MagicMock, connect: MagicMock) -> None:
        expected = connect.return_value
        actual = base.create_tempfile_connection()
        self.assertEqual(actual, expected)
        create_temporary_db_file.assert_called_once_with()
        connect.assert_called_once_with(create_temporary_db_file.return_value.name)


class TidyConnectionTestCase(TestCase):
    @patch("sqlitecollections.base.create_tempfile_connection")
    def test_tidy_connection_calls_create_tempfile_connection_if_none(
        self, create_tempfile_connection: MagicMock
    ) -> None:
        expected = create_tempfile_connection.return_value
        actual = base.tidy_connection(None)
        self.assertEqual(actual, expected)
        create_tempfile_connection.assert_called_once_with()

    @patch("sqlitecollections.base.sqlite3")
    def test_tidy_connection_calls_sqlite3_connection_if_str(self, sqlite3: MagicMock) -> None:
        expected = sqlite3.connect.return_value
        actual = base.tidy_connection("somestring")
        self.assertEqual(actual, expected)
        sqlite3.connect.assert_called_once_with("somestring")

    def test_tidy_connection_do_nothing_with_sqlite3(self) -> None:
        arg = MagicMock(spec=sqlite3.Connection)
        expected = arg
        actual = base.tidy_connection(arg)
        self.assertEqual(actual, expected)

    def test_tidy_conneciton_raises_type_error_for_wrong_types(self) -> None:
        with self.assertRaisesRegex(
            TypeError, r"connection argument must be None or a string or a sqlite3.Connection, not .*"
        ):
            _ = base.tidy_connection(123)  # type: ignore
