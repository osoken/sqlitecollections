import os
import pickle
import sqlite3
import sys
import warnings
from collections.abc import Hashable
from typing import Any
from unittest.mock import MagicMock, patch

from sqlitecollections.base import PicklingStrategy

if sys.version_info >= (3, 9):
    from collections.abc import Callable
else:
    from typing import Callable

from test_base import SqlTestCase

import sqlitecollections as sc


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

    @patch("sqlitecollections.base.SqliteCollectionBase.table_name", return_value="items")
    @patch("sqlitecollections.base.SqliteCollectionBase.__init__", return_value=None)
    @patch("sqlitecollections.base.SqliteCollectionBase.__del__", return_value=None)
    def test_init(
        self, SqliteCollectionBase_del: MagicMock, SqliteCollectionBase_init: MagicMock, _table_name: MagicMock
    ) -> None:
        memory_db = sqlite3.connect(":memory:")
        table_name = "items"
        serializer = MagicMock(spec=Callable[[Hashable], bytes])
        deserializer = MagicMock(spec=Callable[[bytes], Hashable])
        persist = False
        sut = sc.Set[Hashable](
            connection=memory_db,
            table_name=table_name,
            serializer=serializer,
            deserializer=deserializer,
            persist=persist,
            pickling_strategy=PicklingStrategy.only_file_name,
        )
        SqliteCollectionBase_init.assert_called_once_with(
            connection=memory_db,
            table_name=table_name,
            serializer=serializer,
            deserializer=deserializer,
            persist=persist,
            pickling_strategy=PicklingStrategy.only_file_name,
        )

    def test_initialize(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        self.assert_items_table_only(memory_db)
        self.assert_db_state_equals(
            memory_db,
            [],
        )

    def test_init_with_kwarg_data_raises_error(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        with self.assertRaisesRegex(TypeError, ".+ got an unexpected keyword argument 'data'"):
            _ = sc.Set[Hashable](connection=memory_db, table_name="items", data=["a", "b", "a", "a", "aa", b"bb"])  # type: ignore

    def test_init_with_initial_data(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        sut = sc.Set[Hashable](
            ["a", "b", "a", "a", "aa", b"bb"],
            connection=memory_db,
            table_name="items",
        )
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("aa"),),
                (sc.base.SqliteCollectionBase._default_serializer(b"bb"),),
            ],
        )
        sut = sc.Set[Hashable](
            ["a"],
            connection=memory_db,
            table_name="items",
        )
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
            ],
        )

    def test_len(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        expected = 0
        actual = len(sut)
        self.assertEqual(actual, expected)
        self.get_fixture(memory_db, "set/len.sql")
        expected = 2
        actual = len(sut)
        self.assertEqual(actual, expected)

    def test_contains(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/contains.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
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
        self.get_fixture(memory_db, "set/base.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        actual = iter(sut)
        expected: Sequence[Hashable] = []
        self.assertEqual(list(actual), expected)
        self.get_fixture(memory_db, "set/iter.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        actual = iter(sut)
        expected = sorted(["a", "b", "c"])
        self.assertEqual(sorted(list(actual)), expected)
        self.assert_items_table_only(memory_db)

    def test_isdisjoint(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/isdisjoint.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        self.assertFalse(sut.isdisjoint({"a", "b"}))
        self.assertTrue(sut.isdisjoint({1, 2, 3}))
        self.assertTrue(sut.isdisjoint({}))
        self.assertFalse(sut.isdisjoint(sut))
        self.assert_items_table_only(memory_db)

    def test_issubset(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/issubset.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        self.assertFalse(sut.issubset({"a"}))
        self.assertTrue(sut.issubset({"a", "b", "c", "d"}))
        self.assertTrue(sut.issubset(sut))
        self.assert_items_table_only(memory_db)

    def test_intersection_update(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/intersection_update.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut.intersection_update([1, 2, 3])
        self.assert_db_state_equals(memory_db, [])
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/intersection_update.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut.intersection_update(["a", "b"], ["b"])
        self.assert_db_state_equals(memory_db, [(sc.base.SqliteCollectionBase._default_serializer("b"),)])
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/intersection_update.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut.intersection_update(sut)
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        self.assert_items_table_only(memory_db)

    def test_intersection(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/intersection.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        actual = sut.intersection([1, 2, 3])
        self.assert_sql_result_equals(memory_db, f"SELECT serialized_value FROM {actual.table_name}", [])
        actual = sut.intersection(["a", "b"], ["b"])
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [(sc.base.SqliteCollectionBase._default_serializer("b"),)],
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_le(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/le.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        self.assertFalse(sut <= {"a"})
        self.assertTrue(sut <= {"a", "b", "c", "d"})
        self.assertTrue(sut <= sut)
        self.assert_items_table_only(memory_db)

    def test_lt(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/lt.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        self.assertFalse(sut < {"a"})
        self.assertTrue(sut < {"a", "b", "c", "d"})
        self.assertFalse(sut < sut)
        self.assert_items_table_only(memory_db)

    def test_issuperset(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/issuperset.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        self.assertTrue(sut.issuperset({"a"}))
        self.assertFalse(sut.issuperset({"a", "b", "c", "d"}))
        self.assertFalse(sut.issuperset([1]))
        self.assertTrue(sut.issuperset(sut))
        self.assert_items_table_only(memory_db)

    def test_union(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/union.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        actual = sut.union([1, 2, 3])
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
                (sc.base.SqliteCollectionBase._default_serializer(1),),
                (sc.base.SqliteCollectionBase._default_serializer(2),),
                (sc.base.SqliteCollectionBase._default_serializer(3),),
            ],
        )

        actual = sut.union(["a", "b"], ["b"])
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_update(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/update.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut.update([1, 2, 3])
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
                (sc.base.SqliteCollectionBase._default_serializer(1),),
                (sc.base.SqliteCollectionBase._default_serializer(2),),
                (sc.base.SqliteCollectionBase._default_serializer(3),),
            ],
        )
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/update.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut.update(["a", "b"], ["b"])
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/update.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut.update(sut)
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        self.assert_items_table_only(memory_db)

    def test_ge(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/ge.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        self.assertTrue(sut >= {"a"})
        self.assertFalse(sut >= {"a", "b", "c", "d"})
        self.assertTrue(sut >= sut)
        self.assert_items_table_only(memory_db)

    def test_gt(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/gt.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        self.assertTrue(sut > {"a"})
        self.assertFalse(sut > {"a", "b", "c", "d"})
        self.assertFalse(sut > sut)
        self.assert_items_table_only(memory_db)

    def test_or(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/or.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        actual = sut | {1, 2, 3}

        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
                (sc.base.SqliteCollectionBase._default_serializer(1),),
                (sc.base.SqliteCollectionBase._default_serializer(2),),
                (sc.base.SqliteCollectionBase._default_serializer(3),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual = sut | {"a", "b"} | {"b"}
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_and(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/and.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
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
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_difference(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/difference.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        actual = sut.difference([1, 2, 3])

        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual = sut.difference(["a", "b"], {"b"})
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_copy(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/copy.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        actual = sut.copy()

        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_difference_update(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/difference_update.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut.difference_update([1, 2, 3])
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/difference_update.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut.difference_update(["a", "b"], ["b"])
        self.assert_db_state_equals(memory_db, [(sc.base.SqliteCollectionBase._default_serializer("c"),)])
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/difference_update.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut.difference_update(sut)
        self.assert_db_state_equals(memory_db, [])
        self.assert_items_table_only(memory_db)

    def test_sub(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/sub.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        actual = sut - {1, 2, 3}

        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual = sut - {"a", "b"} - {"b"}
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_symmetric_difference(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/symmetric_difference.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        actual = sut.symmetric_difference([1, 2, 3])

        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
                (sc.base.SqliteCollectionBase._default_serializer(1),),
                (sc.base.SqliteCollectionBase._default_serializer(2),),
                (sc.base.SqliteCollectionBase._default_serializer(3),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual = sut.symmetric_difference(["a", "b"], {"b"})
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_symmetric_difference_update(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/symmetric_difference_update.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut.symmetric_difference_update([1, 2, 3])
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
                (sc.base.SqliteCollectionBase._default_serializer(1),),
                (sc.base.SqliteCollectionBase._default_serializer(2),),
                (sc.base.SqliteCollectionBase._default_serializer(3),),
            ],
        )
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/symmetric_difference_update.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut.symmetric_difference_update(["a", "b"], ["b"])
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/symmetric_difference_update.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut.symmetric_difference_update(sut)
        self.assert_db_state_equals(memory_db, [])
        self.assert_items_table_only(memory_db)

    def test_xor(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/xor.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        actual = sut ^ {1, 2, 3}

        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
                (sc.base.SqliteCollectionBase._default_serializer(1),),
                (sc.base.SqliteCollectionBase._default_serializer(2),),
                (sc.base.SqliteCollectionBase._default_serializer(3),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

        actual = sut ^ {"a", "b"} ^ {"b"}
        self.assert_sql_result_equals(
            memory_db,
            f"SELECT serialized_value FROM {actual.table_name}",
            [
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        del actual
        self.assert_items_table_only(memory_db)

    def test_ixor(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/ixor.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut ^= {1, 2, 3}
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
                (sc.base.SqliteCollectionBase._default_serializer(1),),
                (sc.base.SqliteCollectionBase._default_serializer(2),),
                (sc.base.SqliteCollectionBase._default_serializer(3),),
            ],
        )
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/ixor.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut ^= {"b", "d"}
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
                (sc.base.SqliteCollectionBase._default_serializer("d"),),
            ],
        )
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/ixor.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut ^= sut
        self.assert_db_state_equals(memory_db, [])
        self.assert_items_table_only(memory_db)

    def test_ior(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/ior.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut |= {1, 2, 3}
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
                (sc.base.SqliteCollectionBase._default_serializer(1),),
                (sc.base.SqliteCollectionBase._default_serializer(2),),
                (sc.base.SqliteCollectionBase._default_serializer(3),),
            ],
        )
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/ior.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut |= {"b", "d"}
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
                (sc.base.SqliteCollectionBase._default_serializer("d"),),
            ],
        )
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/ior.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut |= sut
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        self.assert_items_table_only(memory_db)

    def test_iand(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/iand.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut &= {1, 2, 3}
        self.assert_db_state_equals(
            memory_db,
            [],
        )
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/iand.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut &= {"b", "d"}
        self.assert_db_state_equals(
            memory_db,
            [(sc.base.SqliteCollectionBase._default_serializer("b"),)],
        )
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/iand.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut &= sut
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        self.assert_items_table_only(memory_db)

    def test_isub(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/isub.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut -= {1, 2, 3}
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/isub.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut -= {"b", "d"}
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/isub.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut -= sut
        self.assert_db_state_equals(
            memory_db,
            [],
        )
        self.assert_items_table_only(memory_db)

    def test_remove(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/remove.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut.remove("a")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        self.assert_items_table_only(memory_db)

        with self.assertRaisesRegex(KeyError, "1"):
            sut.remove(1)

    def test_discard(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/discard.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        sut.discard("a")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        self.assert_items_table_only(memory_db)
        sut.discard(1)
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        self.assert_items_table_only(memory_db)

    def test_pop(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/pop.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        if sys.version_info >= (3, 9):
            expected: set[Hashable] = {"a", "b", "c"}
        else:
            from typing import Set as typing_Set

            expected: typing_Set[Hashable] = {"a", "b", "c"}
        actual = sut.pop()
        expected.remove(actual)
        self.assert_db_state_equals(
            memory_db, [(sc.base.SqliteCollectionBase._default_serializer(d),) for d in expected]
        )
        self.assert_items_table_only(memory_db)
        actual = sut.pop()
        expected.remove(actual)
        self.assert_db_state_equals(
            memory_db, [(sc.base.SqliteCollectionBase._default_serializer(d),) for d in expected]
        )
        self.assert_items_table_only(memory_db)
        actual = sut.pop()
        expected.remove(actual)
        self.assert_db_state_equals(
            memory_db, [(sc.base.SqliteCollectionBase._default_serializer(d),) for d in expected]
        )
        self.assert_items_table_only(memory_db)
        with self.assertRaisesRegex(KeyError, "'pop from an empty set'"):
            sut.pop()

        self.assert_db_state_equals(memory_db, [])

    def test_clear(self) -> None:
        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        self.assert_db_state_equals(
            memory_db,
            [],
        )
        sut.clear()
        self.assert_db_state_equals(
            memory_db,
            [],
        )
        self.assert_items_table_only(memory_db)

        memory_db = sqlite3.connect(":memory:")
        self.get_fixture(memory_db, "set/base.sql", "set/clear.sql")
        sut = sc.Set[Hashable](connection=memory_db, table_name="items")
        self.assert_db_state_equals(
            memory_db,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
            ],
        )
        sut.clear()
        self.assert_db_state_equals(
            memory_db,
            [],
        )
        self.assert_items_table_only(memory_db)

    def test_pickle_with_whole_table_strategy(self) -> None:
        wd = os.path.dirname(os.path.abspath(__file__))

        db = sqlite3.connect(os.path.join(wd, "fixtures", "set", "pickle.db"))
        if sys.version_info < (3, 7):
            sut = sc.Set(connection=db, table_name="items")  # type: ignore
        else:
            sut = sc.Set[str](connection=db, table_name="items")
        actual = pickle.dumps(sut)
        loaded = pickle.loads(actual)
        self.assert_db_state_equals(
            loaded.connection,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )

    def test_pickle_with_only_file_name_strategy(self) -> None:
        wd = os.path.dirname(os.path.abspath(__file__))

        db = sqlite3.connect(os.path.join(wd, "fixtures", "set", "pickle.db"))
        if sys.version_info < (3, 7):
            sut = sc.Set(connection=db, table_name="items", pickling_strategy=PicklingStrategy.only_file_name)  # type: ignore
        else:
            sut = sc.Set[str](connection=db, table_name="items", pickling_strategy=PicklingStrategy.only_file_name)
        actual = pickle.dumps(sut)
        loaded = pickle.loads(actual)
        self.assert_db_state_equals(
            loaded.connection,
            [
                (sc.base.SqliteCollectionBase._default_serializer("a"),),
                (sc.base.SqliteCollectionBase._default_serializer("b"),),
                (sc.base.SqliteCollectionBase._default_serializer("c"),),
            ],
        )
        self.assertEqual(
            sut._driver_class.get_db_filename(sut.connection.cursor()),
            loaded._driver_class.get_db_filename(loaded.connection.cursor()),
        )
