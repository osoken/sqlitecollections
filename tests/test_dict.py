import os
import pickle
import sqlite3
import sys
import warnings
from collections.abc import Hashable
from types import MappingProxyType
from typing import Any
from unittest.mock import MagicMock, patch

if sys.version_info >= (3, 9):
    from collections.abc import Callable, ItemsView, Iterator, KeysView, ValuesView
else:
    from typing import ItemsView, KeysView, ValuesView, Iterator, Callable

from test_base import SqlTestCase

import sqlitecollections as sc
from sqlitecollections.base import PicklingStrategy


class DictAndViewTestCase(SqlTestCase):
    def assert_items_table_only(self, conn: sqlite3.Connection) -> None:
        return self.assert_metadata_state_equals(conn, [("items", "0", "Dict")])


class DictTestCase(DictAndViewTestCase):
    def assert_dict_state_equals(self, conn: sqlite3.Connection, expected: Any) -> None:
        return self.assert_sql_result_equals(
            conn,
            """
                SELECT serialized_key, serialized_value, item_order
                FROM items ORDER BY item_order
            """,
            expected,
        )

    def test_serializer_argument_raises_error(
        self,
    ) -> None:
        def serializer(x: str) -> bytes:
            return x.encode("utf-8")

        memory_db = sqlite3.connect(":memory:")
        with self.assertRaisesRegex(TypeError, ".+ got an unexpected keyword argument 'serializer'"):
            _ = sc.Dict[Hashable, Any](connection=memory_db, table_name="items", serializer=serializer)  # type: ignore

    def test_deserializer_argument_raises_error(
        self,
    ) -> None:
        def deserializer(x: bytes) -> str:
            return x.decode("utf-8")

        memory_db = sqlite3.connect(":memory:")
        with self.assertRaisesRegex(TypeError, ".+ got an unexpected keyword argument 'deserializer'"):
            _ = sc.Dict[Hashable, Any](connection=memory_db, table_name="items", deserializer=deserializer)  # type: ignore

    @patch("sqlitecollections.Dict.table_name", return_value="items")
    @patch("sqlitecollections.Dict._initialize", return_value=None)
    @patch("sqlitecollections.base.SqliteCollectionBase.__init__", return_value=None)
    @patch("sqlitecollections.base.SqliteCollectionBase.__del__", return_value=None)
    def test_init(
        self,
        SqliteCollectionBase_del: MagicMock,
        SqliteCollectionBase_init: MagicMock,
        _initialize: MagicMock,
        _table_name: MagicMock,
    ) -> None:
        memory_db = sqlite3.connect(":memory:")
        table_name = "items"
        value_serializer = MagicMock(spec=Callable[[Any], bytes])
        value_deserializer = MagicMock(spec=Callable[[bytes], Any])
        key_serializer = MagicMock(spec=Callable[[Hashable], bytes])
        key_deserializer = MagicMock(spec=Callable[[bytes], Hashable])
        persist = False
        pickling_strategy = PicklingStrategy.whole_table
        sut = sc.Dict[Hashable, Any](
            connection=memory_db,
            table_name=table_name,
            value_serializer=value_serializer,
            value_deserializer=value_deserializer,
            key_serializer=key_serializer,
            key_deserializer=key_deserializer,
            persist=persist,
            pickling_strategy=pickling_strategy,
        )
        SqliteCollectionBase_init.assert_called_once_with(
            connection=memory_db,
            table_name=table_name,
            serializer=key_serializer,
            deserializer=key_deserializer,
            persist=persist,
            pickling_strategy=pickling_strategy,
        )
        self.assertEqual(sut.value_serializer, value_serializer)
        self.assertEqual(sut.value_deserializer, value_deserializer)

    def test_initialize(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        self.assert_sql_result_equals(
            memory_db,
            "SELECT table_name, schema_version, container_type FROM metadata",
            [
                (
                    "items",
                    sut._driver_class.schema_version,
                    sut.container_type_name,
                ),
            ],
        )
        self.assert_dict_state_equals(
            memory_db,
            [],
        )

    def test_init_with_kwarg_data_raises_error(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        with self.assertRaisesRegex(TypeError, ".+ got an unexpected keyword argument 'data'"):
            _ = sc.Dict[Hashable, Any](connection=memory_db, table_name="items", data=(("a", 1), ("b", 2)))  # type: ignore

    def test_init_with_initial_data(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = sc.Dict[Hashable, Any]((("a", 1), ("b", 2)), connection=memory_db, table_name="items")
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(1),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
            ],
        )
        sut = sc.Dict[Hashable, Any]((("c", 3), ("d", 4)), connection=memory_db, table_name="items")
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("c"),
                    sc.base.SqliteCollectionBase._default_serializer(3),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("d"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    1,
                ),
            ],
        )

    def test_getitem(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/getitem.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
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
        self.get_fixture(memory_db, "dict/base.sql", "dict/delitem.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        del sut["b"]
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    0,
                ),
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
        self.get_fixture(memory_db, "dict/base.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        self.assert_dict_state_equals(
            memory_db,
            [],
        )
        sut["akey"] = {"a": "dict"}
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("akey"),
                    sc.base.SqliteCollectionBase._default_serializer({"a": "dict"}),
                    0,
                )
            ],
        )
        sut["anotherkey"] = ["a", "b"]
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("akey"),
                    sc.base.SqliteCollectionBase._default_serializer({"a": "dict"}),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("anotherkey"),
                    sc.base.SqliteCollectionBase._default_serializer(["a", "b"]),
                    1,
                ),
            ],
        )
        sut["akey"] = None
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("akey"),
                    sc.base.SqliteCollectionBase._default_serializer(None),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("anotherkey"),
                    sc.base.SqliteCollectionBase._default_serializer(["a", "b"]),
                    1,
                ),
            ],
        )
        with self.assertRaisesRegex(TypeError, r"unhashable type:"):
            sut[[0, 1]] = 0  # type: ignore

    def test_len(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        expected = 0
        actual = len(sut)
        self.assertEqual(actual, expected)
        self.get_fixture(memory_db, "dict/len.sql")
        expected = 4
        actual = len(sut)
        self.assertEqual(actual, expected)

    def test_contains(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/contains.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
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
        self.get_fixture(memory_db, "dict/base.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), [])
        self.assertEqual(list(actual), [])
        self.get_fixture(memory_db, "dict/iter.sql")
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), ["a", "b", "c", "d"])

    def test_clear(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        self.assert_dict_state_equals(memory_db, [])
        sut.clear()
        self.assert_dict_state_equals(memory_db, [])
        self.get_fixture(memory_db, "dict/clear.sql")
        sut = sc.Dict(connection=memory_db, table_name="items")
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    0,
                )
            ],
        )
        sut.clear()
        self.assert_dict_state_equals(memory_db, [])

    def test_get(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/get.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
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
        self.get_fixture(memory_db, "dict/base.sql", "dict/items.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        actual = sut.items()
        self.assertIsInstance(actual, ItemsView)
        expected = [("a", 4), ("b", 2)]
        self.assertEqual(list(actual), expected)

    def test_keys(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keys.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        actual = sut.keys()
        self.assertIsInstance(actual, KeysView)
        expected = ["a", "b"]
        self.assertEqual(list(actual), expected)

    def test_pop(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/pop.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("c"),
                    sc.base.SqliteCollectionBase._default_serializer(None),
                    2,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("d"),
                    sc.base.SqliteCollectionBase._default_serializer([1, 2]),
                    3,
                ),
            ],
        )
        expected1 = 4
        actual1 = sut.pop("a")
        self.assertEqual(actual1, expected1)
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("c"),
                    sc.base.SqliteCollectionBase._default_serializer(None),
                    2,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("d"),
                    sc.base.SqliteCollectionBase._default_serializer([1, 2]),
                    3,
                ),
            ],
        )
        expected2 = "default"
        actual2 = sut.pop("nonsuch", "default")
        self.assertEqual(actual2, expected2)
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("c"),
                    sc.base.SqliteCollectionBase._default_serializer(None),
                    2,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("d"),
                    sc.base.SqliteCollectionBase._default_serializer([1, 2]),
                    3,
                ),
            ],
        )
        with self.assertRaisesRegex(KeyError, "nonsuch"):
            _ = sut.pop("nonsuch")
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("c"),
                    sc.base.SqliteCollectionBase._default_serializer(None),
                    2,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("d"),
                    sc.base.SqliteCollectionBase._default_serializer([1, 2]),
                    3,
                ),
            ],
        )

    def test_popitem(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/popitem.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
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
                    (
                        sc.base.SqliteCollectionBase._default_serializer("a"),
                        sc.base.SqliteCollectionBase._default_serializer(4),
                        0,
                    ),
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
        self.get_fixture(memory_db, "dict/base.sql", "dict/reversed.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
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
        self.get_fixture(memory_db, "dict/base.sql", "dict/setdefault.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
            ],
        )
        expected1 = 4
        actual1 = sut.setdefault("a")
        self.assertEqual(actual1, expected1)
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
            ],
        )
        expected2 = None
        actual2 = sut.setdefault("c")
        self.assertEqual(actual2, expected2)
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("c"),
                    sc.base.SqliteCollectionBase._default_serializer(None),
                    2,
                ),
            ],
        )
        expected3 = 2
        actual3 = sut.setdefault("b", "default")
        self.assertEqual(actual3, expected3)
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("c"),
                    sc.base.SqliteCollectionBase._default_serializer(None),
                    2,
                ),
            ],
        )
        expected4 = "default"
        actual4 = sut.setdefault("d", "default")
        self.assertEqual(actual4, expected4)
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("c"),
                    sc.base.SqliteCollectionBase._default_serializer(None),
                    2,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("d"),
                    sc.base.SqliteCollectionBase._default_serializer("default"),
                    3,
                ),
            ],
        )

    def test_update(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/update.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut.update({"a": 1, "e": 10})
        self.assert_dict_state_equals(
            memory_db,
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(1),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("e"),
                    sc.base.SqliteCollectionBase._default_serializer(10),
                    2,
                ),
            ],
        )

    def test_values(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/values.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        actual = sut.values()
        self.assertIsInstance(actual, ValuesView)
        expected = [4, 2]
        self.assertEqual(list(actual), expected)

    def test_or(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/or.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
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
                    (
                        sc.base.SqliteCollectionBase._default_serializer("a"),
                        sc.base.SqliteCollectionBase._default_serializer(1),
                        0,
                    ),
                    (
                        sc.base.SqliteCollectionBase._default_serializer("b"),
                        sc.base.SqliteCollectionBase._default_serializer(2),
                        1,
                    ),
                    (
                        sc.base.SqliteCollectionBase._default_serializer("e"),
                        sc.base.SqliteCollectionBase._default_serializer(10),
                        2,
                    ),
                ],
            )

    def test_ior(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/ior.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
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
                    (
                        sc.base.SqliteCollectionBase._default_serializer("a"),
                        sc.base.SqliteCollectionBase._default_serializer(1),
                        0,
                    ),
                    (
                        sc.base.SqliteCollectionBase._default_serializer("b"),
                        sc.base.SqliteCollectionBase._default_serializer(2),
                        1,
                    ),
                    (
                        sc.base.SqliteCollectionBase._default_serializer("e"),
                        sc.base.SqliteCollectionBase._default_serializer(10),
                        2,
                    ),
                ],
            )

    def test_copy(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/copy.sql")
        sut = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        actual = sut.copy()

        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_key, serialized_value, item_order FROM {actual.table_name}",
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(4),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    1,
                ),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_pickle_with_whole_table_strategy(self) -> None:

        wd = os.path.dirname(os.path.abspath(__file__))

        db = sqlite3.connect(os.path.join(wd, "fixtures", "dict", "pickle.db"))
        if sys.version_info < (3, 7):
            sut = sc.Dict(connection=db, table_name="items", pickling_strategy=PicklingStrategy.whole_table)  # type: ignore
        else:
            sut = sc.Dict[str, int](connection=db, table_name="items", pickling_strategy=PicklingStrategy.whole_table)
        actual = pickle.dumps(sut)
        loaded = pickle.loads(actual)
        self.assert_sql_result_equals(
            loaded.connection,
            f"SELECT serialized_key, serialized_value, item_order FROM {sut.table_name}",
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(0),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(1),
                    1,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("c"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    2,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("d"),
                    sc.base.SqliteCollectionBase._default_serializer(3),
                    3,
                ),
            ],
        )

    def test_pickle_with_only_file_name_strategy(self) -> None:

        wd = os.path.dirname(os.path.abspath(__file__))

        db = sqlite3.connect(os.path.relpath(os.path.join(wd, "fixtures", "dict", "pickle.db")))
        if sys.version_info < (3, 7):
            sut = sc.Dict(connection=db, table_name="items", pickling_strategy=PicklingStrategy.only_file_name)  # type: ignore
        else:
            sut = sc.Dict[str, int](
                connection=db, table_name="items", pickling_strategy=PicklingStrategy.only_file_name
            )
        actual = pickle.dumps(sut)
        loaded = pickle.loads(actual)
        self.assert_sql_result_equals(
            loaded.connection,
            f"SELECT serialized_key, serialized_value, item_order FROM {sut.table_name}",
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer("a"),
                    sc.base.SqliteCollectionBase._default_serializer(0),
                    0,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("b"),
                    sc.base.SqliteCollectionBase._default_serializer(1),
                    1,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("c"),
                    sc.base.SqliteCollectionBase._default_serializer(2),
                    2,
                ),
                (
                    sc.base.SqliteCollectionBase._default_serializer("d"),
                    sc.base.SqliteCollectionBase._default_serializer(3),
                    3,
                ),
            ],
        )
        self.assertEqual(
            sut._driver_class.get_db_filename(sut.connection.cursor()),
            loaded._driver_class.get_db_filename(loaded.connection.cursor()),
        )

    @patch("sqlitecollections.dict.tidy_connection")
    def test_pickle_with_only_file_name_strategy_serializes_the_relpath(self, tidy_connection: MagicMock) -> None:

        wd = os.path.dirname(os.path.abspath(__file__))
        relpath = os.path.relpath(os.path.join(wd, "fixtures", "dict", "pickle.db"))
        db = sqlite3.connect(relpath)
        tidy_connection.return_value = db

        if sys.version_info < (3, 7):
            sut = sc.Dict(connection=db, table_name="items", pickling_strategy=PicklingStrategy.only_file_name)  # type: ignore
        else:
            sut = sc.Dict[str, int](
                connection=db, table_name="items", pickling_strategy=PicklingStrategy.only_file_name
            )
        _ = pickle.loads(pickle.dumps(sut))
        tidy_connection.assert_called_once_with(relpath)

    def test_pickle_with_only_file_name_strategy_raises_error_when_connection_is_on_memory(self) -> None:
        if sys.version_info < (3, 7):
            sut = sc.Dict(connection=":memory:", table_name="items", pickling_strategy=PicklingStrategy.only_file_name)  # type: ignore
        else:
            sut = sc.Dict[str, int](
                connection=":memory:", table_name="items", pickling_strategy=PicklingStrategy.only_file_name
            )
        with self.assertRaisesRegex(ValueError, r"no path specified"):
            _ = pickle.dumps(sut)


class KeysViewTestCase(DictAndViewTestCase):
    def test_len(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        expected = 0
        actual = len(sut)
        self.assertEqual(actual, expected)

        self.get_fixture(memory_db, "dict/keysview_len.sql")
        expected = 4
        actual = len(sut)
        self.assertEqual(actual, expected)

    def test_iter(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), [])
        self.assertEqual(list(actual), [])
        self.get_fixture(memory_db, "dict/keysview_iter.sql")
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), ["a", "b", "c", "d"])
        del parent["b"]
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), ["a", "c", "d"])

    def test_contains(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keysview_contains.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
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

        del parent["a"]
        self.assertFalse("a" in sut)

    def test_reversed(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keysview_reversed.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        if sys.version_info < (3, 8):
            with self.assertRaisesRegex(TypeError, "'KeysView' object is not reversible"):
                _ = reversed(sut)  # type: ignore
        else:
            actual = reversed(sut)
            self.assertIsInstance(actual, Iterator)
            expected = ["b", "a"]
            self.assertEqual(list(actual), expected)

    def test_and(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keysview_and.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        actual = sut & {1, 2, 3}
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [],
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual = sut & {"a", "b"} & {"b"}
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_rand(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keysview_rand.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        actual = iter((1, 2, 3)) & sut
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [],
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual2 = iter(("a", "b")) & sut
        self.assertIsInstance(actual2, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual2.table_name}",
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
            ],
        )
        del actual2
        self.assert_items_table_only(memory_db)

    def test_or(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keysview_or.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        actual = sut | iter((1, 2, 3))
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer("a"),),
                    (sc.base.SqliteCollectionBase._default_serializer("b"),),
                    (sc.base.SqliteCollectionBase._default_serializer("c"),),
                    (sc.base.SqliteCollectionBase._default_serializer(1),),
                    (sc.base.SqliteCollectionBase._default_serializer(2),),
                    (sc.base.SqliteCollectionBase._default_serializer(3),),
                ]
            ),
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual2 = sut | iter(("a", "b"))
        self.assertIsInstance(actual2, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual2.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer("a"),),
                    (sc.base.SqliteCollectionBase._default_serializer("b"),),
                    (sc.base.SqliteCollectionBase._default_serializer("c"),),
                ]
            ),
        )
        del actual2
        self.assert_items_table_only(memory_db)

    def test_ror(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keysview_ror.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        actual = iter((1, 2, 3)) | sut
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer("a"),),
                    (sc.base.SqliteCollectionBase._default_serializer("b"),),
                    (sc.base.SqliteCollectionBase._default_serializer("c"),),
                    (sc.base.SqliteCollectionBase._default_serializer(1),),
                    (sc.base.SqliteCollectionBase._default_serializer(2),),
                    (sc.base.SqliteCollectionBase._default_serializer(3),),
                ]
            ),
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual2 = iter(("a", "b")) | sut
        self.assertIsInstance(actual2, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual2.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer("a"),),
                    (sc.base.SqliteCollectionBase._default_serializer("b"),),
                    (sc.base.SqliteCollectionBase._default_serializer("c"),),
                ]
            ),
        )
        del actual2
        self.assert_items_table_only(memory_db)

    def test_sub(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keysview_sub.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        actual = sut - iter((1, 2, 3))
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer("a"),),
                    (sc.base.SqliteCollectionBase._default_serializer("b"),),
                    (sc.base.SqliteCollectionBase._default_serializer("c"),),
                ]
            ),
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual2 = sut - iter(("a", "b"))
        self.assertIsInstance(actual2, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual2.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer("c"),),
                ]
            ),
        )
        del actual2
        self.assert_items_table_only(memory_db)

    def test_rsub(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keysview_rsub.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        actual = iter((1, 2, 3)) - sut
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer(1),),
                    (sc.base.SqliteCollectionBase._default_serializer(2),),
                    (sc.base.SqliteCollectionBase._default_serializer(3),),
                ]
            ),
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual2 = iter(("a", "b")) - sut
        self.assertIsInstance(actual2, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual2.table_name} ORDER BY serialized_value",
            sorted([]),
        )
        del actual2
        self.assert_items_table_only(memory_db)

    def test_xor(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keysview_xor.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        actual = sut ^ iter((1, 2, 3))
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer("a"),),
                    (sc.base.SqliteCollectionBase._default_serializer("b"),),
                    (sc.base.SqliteCollectionBase._default_serializer("c"),),
                    (sc.base.SqliteCollectionBase._default_serializer(1),),
                    (sc.base.SqliteCollectionBase._default_serializer(2),),
                    (sc.base.SqliteCollectionBase._default_serializer(3),),
                ]
            ),
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual2 = sut ^ iter(("a", "b", "d"))
        self.assertIsInstance(actual2, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual2.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer("c"),),
                    (sc.base.SqliteCollectionBase._default_serializer("d"),),
                ]
            ),
        )
        del actual2
        self.assert_items_table_only(memory_db)

    def test_rxor(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keysview_rxor.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        actual = iter((1, 2, 3)) ^ sut
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer("a"),),
                    (sc.base.SqliteCollectionBase._default_serializer("b"),),
                    (sc.base.SqliteCollectionBase._default_serializer("c"),),
                    (sc.base.SqliteCollectionBase._default_serializer(1),),
                    (sc.base.SqliteCollectionBase._default_serializer(2),),
                    (sc.base.SqliteCollectionBase._default_serializer(3),),
                ]
            ),
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual2 = iter(("a", "b", "d")) ^ sut
        self.assertIsInstance(actual2, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual2.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer("c"),),
                    (sc.base.SqliteCollectionBase._default_serializer("d"),),
                ]
            ),
        )
        del actual2
        self.assert_items_table_only(memory_db)

    def test_mapping(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/keysview_mapping.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        if sys.version_info < (3, 10):
            with self.assertRaisesRegex(AttributeError, "'KeysView' object has no attribute 'mapping'"):
                _ = sut.mapping  # type: ignore
        else:
            actual = sut.mapping  # type: ignore
            self.assertIsInstance(actual, MappingProxyType)
            self.assertEqual(len(actual), 3)
            self.assertEqual(actual["a"], 1)
            self.assertEqual(actual["b"], 2)
            self.assertEqual(actual["c"], 3)

    def test_repr(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_mapping.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.keys()
        self.assertRegex(repr(sut), r"KeysView\(<sqlitecollections\.dict\.Dict object at 0x[0-9a-f]+>\)")


class ValuesViewTestCase(DictAndViewTestCase):
    def test_len(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.values()
        expected = 0
        actual = len(sut)
        self.assertEqual(actual, expected)

        self.get_fixture(memory_db, "dict/valuesview_len.sql")
        expected = 4
        actual = len(sut)
        self.assertEqual(actual, expected)

    def test_contains(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/valuesview_contains.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.values()

        self.assertTrue("a" in sut)
        self.assertTrue(b"a" in sut)
        self.assertTrue(None in sut)
        self.assertTrue(0 in sut)
        self.assertFalse(100 in sut)
        self.assertTrue(((0, 1), "a") in sut)

        self.assertFalse("a" not in sut)
        self.assertFalse(b"a" not in sut)
        self.assertFalse(None not in sut)
        self.assertFalse(0 not in sut)
        self.assertFalse(((0, 1), "a") not in sut)
        self.assertTrue(100 not in sut)

    def test_iter(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.values()
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), [])
        self.assertEqual(list(actual), [])
        self.get_fixture(memory_db, "dict/valuesview_iter.sql")
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), [4, 2, None, [1, 2]])
        del parent["b"]
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), [4, None, [1, 2]])

    def test_reversed(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/valuesview_reversed.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.values()
        if sys.version_info < (3, 8):
            with self.assertRaisesRegex(TypeError, "'ValuesView' object is not reversible"):
                _ = reversed(sut)  # type: ignore
        else:
            actual = reversed(sut)
            self.assertIsInstance(actual, Iterator)
            expected = [2, 4]
            self.assertEqual(list(actual), expected)

    def test_mapping(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/valuesview_mapping.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.values()
        if sys.version_info < (3, 10):
            with self.assertRaisesRegex(AttributeError, "'ValuesView' object has no attribute 'mapping'"):
                _ = sut.mapping  # type: ignore
        else:
            actual = sut.mapping  # type: ignore
            self.assertIsInstance(actual, MappingProxyType)
            self.assertEqual(len(actual), 3)
            self.assertEqual(actual["a"], 1)
            self.assertEqual(actual["b"], 2)
            self.assertEqual(actual["c"], 3)

    def test_repr(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_mapping.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.values()
        self.assertRegex(repr(sut), r"ValuesView\(<sqlitecollections\.dict\.Dict object at 0x[0-9a-f]+>\)")


class ItemsViewTestCase(DictAndViewTestCase):
    def test_len(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        expected = 0
        actual = len(sut)
        self.assertEqual(actual, expected)

        self.get_fixture(memory_db, "dict/itemsview_len.sql")
        expected = 4
        actual = len(sut)
        self.assertEqual(actual, expected)

    def test_iter(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), [])
        self.assertEqual(list(actual), [])
        self.get_fixture(memory_db, "dict/itemsview_iter.sql")
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), [("a", 4), ("b", 2), ("c", None), ("d", [1, 2])])
        del parent["b"]
        actual = iter(sut)
        self.assertIsInstance(actual, Iterator)
        self.assertEqual(list(actual), [("a", 4), ("c", None), ("d", [1, 2])])

    def test_and(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_and.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        actual = sut & {("a", 1), ("b", 2)}
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [],
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual = sut & {("a", 4), ("b", 4)} & {("b", 4)}
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (
                    sc.base.SqliteCollectionBase._default_serializer(
                        (
                            sc.base.SqliteCollectionBase._default_serializer("b"),
                            sc.base.SqliteCollectionBase._default_serializer(4),
                        )
                    ),
                ),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_and_fails_if_unhashable_value_exists(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_and_unhashable_type_error.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        with self.assertRaisesRegex(TypeError, r"unhashable type: '[a-zA-Z0-9_]+'"):
            _ = sut & {("a", 1)}

    def test_rand(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_rand.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        actual = iter((("a", 1), ("b", 2))) & sut
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [],
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual = iter((("a", 4), ("b", 4))) & sut
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (
                        sc.base.SqliteCollectionBase._default_serializer(
                            (
                                sc.base.SqliteCollectionBase._default_serializer("a"),
                                sc.base.SqliteCollectionBase._default_serializer(4),
                            )
                        ),
                    ),
                    (
                        sc.base.SqliteCollectionBase._default_serializer(
                            (
                                sc.base.SqliteCollectionBase._default_serializer("b"),
                                sc.base.SqliteCollectionBase._default_serializer(4),
                            )
                        ),
                    ),
                ]
            ),
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_rand_fails_if_unhashable_value_exists(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_rand_unhashable_type_error.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        with self.assertRaisesRegex(TypeError, r"unhashable type: '[a-zA-Z0-9_]+'"):
            _ = iter(tuple(("a", 1))) & sut

    def test_contains(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_contains.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        self.assertTrue(("a", [4, 5]) in sut)
        self.assertFalse(("a", (4, 5)) in sut)
        self.assertTrue(("b", 4) in sut)
        self.assertFalse(("b", 0) in sut)
        self.assertFalse("a" in sut)  # type: ignore

    def test_or(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_or.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        actual = sut | {("a", 1), ("b", 2)}
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (
                        sc.base.SqliteCollectionBase._default_serializer(
                            (
                                sc.base.SqliteCollectionBase._default_serializer("a"),
                                sc.base.SqliteCollectionBase._default_serializer(4),
                            )
                        ),
                    ),
                    (
                        sc.base.SqliteCollectionBase._default_serializer(
                            (
                                sc.base.SqliteCollectionBase._default_serializer("a"),
                                sc.base.SqliteCollectionBase._default_serializer(1),
                            )
                        ),
                    ),
                    (
                        sc.base.SqliteCollectionBase._default_serializer(
                            (
                                sc.base.SqliteCollectionBase._default_serializer("b"),
                                sc.base.SqliteCollectionBase._default_serializer(2),
                            )
                        ),
                    ),
                ]
            ),
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual = sut | {("a", 4), ("e", 10)}
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (
                        sc.base.SqliteCollectionBase._default_serializer(
                            (
                                sc.base.SqliteCollectionBase._default_serializer("a"),
                                sc.base.SqliteCollectionBase._default_serializer(4),
                            )
                        ),
                    ),
                    (
                        sc.base.SqliteCollectionBase._default_serializer(
                            (
                                sc.base.SqliteCollectionBase._default_serializer("e"),
                                sc.base.SqliteCollectionBase._default_serializer(10),
                            )
                        ),
                    ),
                ]
            ),
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_or_fails_if_unhashable_value_exists(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_or_unhashable_type_error.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        with self.assertRaisesRegex(TypeError, r"unhashable type: '[a-zA-Z0-9_]+'"):
            _ = sut | iter(tuple(("a", 1)))

    def test_ror(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_ror.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        actual = iter((("a", 1), ("b", 2))) | sut
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (
                        sc.base.SqliteCollectionBase._default_serializer(
                            (
                                sc.base.SqliteCollectionBase._default_serializer("a"),
                                sc.base.SqliteCollectionBase._default_serializer(4),
                            )
                        ),
                    ),
                    (
                        sc.base.SqliteCollectionBase._default_serializer(
                            (
                                sc.base.SqliteCollectionBase._default_serializer("a"),
                                sc.base.SqliteCollectionBase._default_serializer(1),
                            )
                        ),
                    ),
                    (
                        sc.base.SqliteCollectionBase._default_serializer(
                            (
                                sc.base.SqliteCollectionBase._default_serializer("b"),
                                sc.base.SqliteCollectionBase._default_serializer(2),
                            )
                        ),
                    ),
                ]
            ),
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual = iter((("a", 4), ("e", 10))) | sut
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (
                        sc.base.SqliteCollectionBase._default_serializer(
                            (
                                sc.base.SqliteCollectionBase._default_serializer("a"),
                                sc.base.SqliteCollectionBase._default_serializer(4),
                            )
                        ),
                    ),
                    (
                        sc.base.SqliteCollectionBase._default_serializer(
                            (
                                sc.base.SqliteCollectionBase._default_serializer("e"),
                                sc.base.SqliteCollectionBase._default_serializer(10),
                            )
                        ),
                    ),
                ]
            ),
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_ror_fails_if_unhashable_value_exists(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_ror_unhashable_type_error.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        with self.assertRaisesRegex(TypeError, r"unhashable type: '[a-zA-Z0-9_]+'"):
            _ = iter(tuple(("a", 1))) | sut

    def test_sub(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_sub.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        actual = sut - iter((("a", 1), ("b", 2), ("c", 3)))
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (
                        sc.base.SqliteCollectionBase._default_serializer(
                            (
                                sc.base.SqliteCollectionBase._default_serializer("a"),
                                sc.base.SqliteCollectionBase._default_serializer(4),
                            )
                        ),
                    ),
                    (
                        sc.base.SqliteCollectionBase._default_serializer(
                            (
                                sc.base.SqliteCollectionBase._default_serializer("b"),
                                sc.base.SqliteCollectionBase._default_serializer(4),
                            )
                        ),
                    ),
                    (
                        sc.base.SqliteCollectionBase._default_serializer(
                            (
                                sc.base.SqliteCollectionBase._default_serializer("c"),
                                sc.base.SqliteCollectionBase._default_serializer(4),
                            )
                        ),
                    ),
                ]
            ),
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual2 = sut - iter((("a", 4), ("b", 0)))
        self.assertIsInstance(actual2, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual2.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (
                        sc.base.SqliteCollectionBase._default_serializer(
                            (
                                sc.base.SqliteCollectionBase._default_serializer("b"),
                                sc.base.SqliteCollectionBase._default_serializer(4),
                            )
                        ),
                    ),
                    (
                        sc.base.SqliteCollectionBase._default_serializer(
                            (
                                sc.base.SqliteCollectionBase._default_serializer("c"),
                                sc.base.SqliteCollectionBase._default_serializer(4),
                            )
                        ),
                    ),
                ]
            ),
        )
        del actual2
        self.assert_items_table_only(memory_db)

    def test_sub_fails_if_unhashable_value_exists(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_sub_unhashable_type_error.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        with self.assertRaisesRegex(TypeError, r"unhashable type: '[a-zA-Z0-9_]+'"):
            _ = sut - iter(tuple(("a", 1)))

    def test_rsub(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_rsub.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        actual = iter((("a", 1), ("b", 2), ("c", 3))) - sut
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer(("a", 1)),),
                    (sc.base.SqliteCollectionBase._default_serializer(("b", 2)),),
                    (sc.base.SqliteCollectionBase._default_serializer(("c", 3)),),
                ]
            ),
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual2 = iter((("a", 4), ("b", 0))) - sut
        self.assertIsInstance(actual2, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual2.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (sc.base.SqliteCollectionBase._default_serializer(("b", 0)),),
                ]
            ),
        )
        del actual2
        self.assert_items_table_only(memory_db)

    def test_rsub_fails_if_unhashable_value_exists(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_rsub_unhashable_type_error.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        with self.assertRaisesRegex(TypeError, r"unhashable type: '[a-zA-Z0-9_]+'"):
            _ = iter(tuple(("a", 1))) - sut

    def test_xor(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_xor.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        actual = sut ^ iter((("a", 4), ("b", 4), ("c", 3)))
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (
                        sc.base.SqliteCollectionBase._default_serializer(
                            (
                                sc.base.SqliteCollectionBase._default_serializer("c"),
                                sc.base.SqliteCollectionBase._default_serializer(4),
                            )
                        ),
                    ),
                    (
                        sc.base.SqliteCollectionBase._default_serializer(
                            (
                                sc.base.SqliteCollectionBase._default_serializer("c"),
                                sc.base.SqliteCollectionBase._default_serializer(3),
                            )
                        ),
                    ),
                ]
            ),
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_xor_fails_if_unhashable_value_exists(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_xor_unhashable_type_error.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        with self.assertRaisesRegex(TypeError, r"unhashable type: '[a-zA-Z0-9_]+'"):
            _ = sut ^ iter(tuple(("a", 1)))

    def test_rxor(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_rxor.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        actual = iter((("a", 4), ("b", 4), ("c", 3))) ^ sut
        self.assertIsInstance(actual, sc.Set)
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name} ORDER BY serialized_value",
            sorted(
                [
                    (
                        sc.base.SqliteCollectionBase._default_serializer(
                            (
                                sc.base.SqliteCollectionBase._default_serializer("c"),
                                sc.base.SqliteCollectionBase._default_serializer(4),
                            )
                        ),
                    ),
                    (
                        sc.base.SqliteCollectionBase._default_serializer(
                            (
                                sc.base.SqliteCollectionBase._default_serializer("c"),
                                sc.base.SqliteCollectionBase._default_serializer(3),
                            )
                        ),
                    ),
                ]
            ),
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_rxor_fails_if_unhashable_value_exists(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_rxor_unhashable_type_error.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        with self.assertRaisesRegex(TypeError, r"unhashable type: '[a-zA-Z0-9_]+'"):
            _ = iter(tuple(("a", 1))) ^ sut

    def test_mapping(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_mapping.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        if sys.version_info < (3, 10):
            with self.assertRaisesRegex(AttributeError, "'ItemsView' object has no attribute 'mapping'"):
                _ = sut.mapping  # type: ignore
        else:
            actual = sut.mapping  # type: ignore
            self.assertIsInstance(actual, MappingProxyType)
            self.assertEqual(len(actual), 3)
            self.assertEqual(actual["a"], 1)
            self.assertEqual(actual["b"], 2)
            self.assertEqual(actual["c"], 3)

    def test_repr(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "dict/base.sql", "dict/itemsview_mapping.sql")
        parent = sc.Dict[Hashable, Any](connection=memory_db, table_name="items")
        sut = parent.items()
        self.assertRegex(repr(sut), r"ItemsView\(<sqlitecollections\.dict\.Dict object at 0x[0-9a-f]+>\)")
