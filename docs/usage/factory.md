# factory module

`factory` module contains a factory for each container:
`ListFactory` for `List`, `SetFactory` for `Set` and `DictFactory` for `Dict`.
They replace constructors of each container that require extra arguments such as `connection`, `serializer`, `deserializer` etc., and allow users to construct a container without specifying those extra arguments.

## Example

The results of the following two examples are almost the same (except for the auto-generated table names).

Without factory:

```
import sqlite3
import sqlitecollections as sc

conn = sqlite3.connect("path/to/file.db")

def encode(x: str) -> bytes:
    return x.encode("utf-8")

def decode(x: bytes) -> str:
    return x.decode("utf-8")

l1 = sc.List[str](connection=conn, serializer=encode, deserializer=decode, data=["Alice", "Bob", "Carol"])
l2 = sc.List[str](connection=conn, serializer=encode, deserializer=decode, data=["Dave"])
l3 = sc.List[str](connection=conn, serializer=encode, deserializer=decode, data=["Erin"])
```

With factory:

```
import sqlite3
from sqlitecollections import factory

conn = sqlite3.connect("path/to/file.db")

def encode(x: str) -> bytes:
    return x.encode("utf-8")

def decode(x: bytes) -> str:
    return x.decode("utf-8")

list_ = factory.ListFactory[str](connection=conn, serializer=encode, deserializer=decode)

l1 = list_(["Alice", "Bob", "Carol"])
l2 = list_(["Dave"])
l3 = list_(["Erin"])
```

# ListFactory

## `ListFactory[T](...)`

Constructor of `ListFactory` which constructs `List`.

### Type Parameters:

- `T`: value type of the `List`

### Arguments:

- `connection`: `str` or `sqlite3.Connection`, optional, default=`None`; If `None`, temporary file is automatically created. If `connection` is a `str`, it will be used as the sqlite3 database file name. You can pass a `sqlite3.Connection` directly.
- `serializer`: `Callable[[T], bytes]`, optional, default=`None`; Function to serialize value. If `None`, `pickle.dumps` is used.
- `deserializer`: `Callable[[bytes], T]`, optional, default=`None`; Function to deserialize value. If `None`, `pickle.loads` is used.

---

## `list_(data)`

Construct `List[T]` with connection, serializer and deserializer preset from `ListFactory[T]` `list_`.

### Arguments:

- `data`: `Iterable[T]`, optional, defualt=`None`; Initial data.

### Return value:

`List[T]`: connection, serializer and deserializer are the same as those of the factory `list_`.

---

## `create(data)`

Construct `List[T]` with connection, serializer and deserializer preset from the factory.

### Arguments:

- `data`: `Iterable[T]`, optional, defualt=`None`; Initial data.

### Return value:

`List[T]`: connection, serializer and deserializer are the same as those of the factory.

===

# DictFactory

## `DictFactory[KT, VT](...)`

Constructor of `DictFactory` which constructs `Dict`.

### Type Parameters:

- `KT`: key type of the `Dict`
- `VT`: value type of the `Dict`

### Arguments:

- `connection`: `str` or `sqlite3.Connection`, optional, default=`None`; If `None`, temporary file is automatically created. If `connection` is a `str`, it will be used as the sqlite3 database file name. You can pass a `sqlite3.Connection` directly.
- `key_serializer`: `Callable[[KT], bytes]`, optional, default=`None`; Function to serialize key. If `None`, `pickle.dumps` is used.
- `key_deserializer`: `Callable[[bytes], KT]`, optional, default=`None`; Function to deserialize key. If `None`, `pickle.loads` is used.
- `value_serializer`: `Callable[[VT], bytes]`, optional, default=`None`; Function to serialize value. If `None`, `key_serializer` is used.
- `value_deserializer`: `Callable[[bytes], VT]`, optional, default=`None`; Function to deserialize value. If `None`, `key_deserializer` is used.

---

## `dict_(data, **kwargs)`

Construct `Dict[KT, VT]` with connection, key*serializer, etc., preset from `DictFactory[KT, VT]` `dict*`.

### Arguments:

- `data`: `Mapping[KT, VT]` or `Iterable[Tuple[KT, VT]]`, optional, defualt=`None`; Initial data.
- `**kwargs`: `VT`, optional; Appended to initial data.

### Return value:

`Dict[KT, VT]`: connection, key*serializers, etc., are the same as those of the factory `dict*`.

---

## `create(data, **kwargs)`

Construct `Dict[KT, VT]` with connection, key_serializer, etc., preset from the factory.

### Arguments:

- `data`: `Mapping[KT, VT]` or `Iterable[Tuple[KT, VT]]`, optional, defualt=`None`; Initial data.
- `**kwargs`: `VT`, optional; Appended to initial data.

### Return value:

`Dict[KT, VT]`: connection, key_serializers, etc., are the same as those of the factory.

===

# SetFactory

## `SetFactory[T](...)`

Constructor of `SetFactory` which constructs `Set`.

### Type Parameters:

- `T`: value type of the `Set`

### Arguments:

- `connection`: `str` or `sqlite3.Connection`, optional, default=`None`; If `None`, temporary file is automatically created. If `connection` is a `str`, it will be used as the sqlite3 database file name. You can pass a `sqlite3.Connection` directly.
- `serializer`: `Callable[[T], bytes]`, optional, default=`None`; Function to serialize value. If `None`, `pickle.dumps` is used.
- `deserializer`: `Callable[[bytes], T]`, optional, default=`None`; Function to deserialize value. If `None`, `pickle.loads` is used.

---

## `set_(data)`

Construct `Set[T]` with connection, serializer and deserializer preset from `SetFactory[T]` `set_`.

### Arguments:

- `data`: `Iterable[T]`, optional, defualt=`None`; Initial data.

### Return value:

`Set[T]`: connection, serializer and deserializer are the same as those of the factory `set_`.

---

## `create(data)`

Construct `Set[T]` with connection, serializer and deserializer preset from the factory.

### Arguments:

- `data`: `Iterable[T]`, optional, defualt=`None`; Initial data.

### Return value:

`Set[T]`: connection, serializer and deserializer are the same as those of the factory.

===
