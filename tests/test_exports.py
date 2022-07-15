from unittest import TestCase
from xmlrpc.client import Boolean

import sqlitecollections as sc
from sqlitecollections import factory


class ImportFactories(TestCase):
    def assert_is_factory_class(self, x: object) -> None:
        self.assertTrue(issubclass(x, factory.FactoryBase))

    def test_list_factory_is_accessible(self) -> None:
        self.assert_is_factory_class(sc.ListFactory)

    def test_import_list_factory(self) -> None:
        from sqlitecollections import ListFactory

        self.assert_is_factory_class(ListFactory)

    def test_dict_factory_is_accessible(self) -> None:
        self.assert_is_factory_class(sc.DictFactory)

    def test_import_dict_factory(self) -> None:
        from sqlitecollections import DictFactory

        self.assert_is_factory_class(DictFactory)

    def test_set_factory_is_accessible(self) -> None:
        self.assert_is_factory_class(sc.SetFactory)

    def test_import_set_factory(self) -> None:
        from sqlitecollections import SetFactory

        self.assert_is_factory_class(SetFactory)
