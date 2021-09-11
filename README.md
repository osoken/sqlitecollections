# sqlitecollections

`sqlitecollections` is a sort of containers that are backended by sqlite3 DB and are compatible with corresponding built-in collections. Since containers consume disk space instead of RAM, they can handle large amounts of data even in environments with limited RAM. Migrating from existing code using the built-in container is as simple as importing the library and changing the constructor.

The elements of the container are automatically serialized and stored in the sqlite3 database, and are automatically read from the sqlite3 database and deserialized when accessed. Current version supports List (mutable sequence), Dict (mutable mapping) and Set (mutable set) and almost all methods are compatible with list, dict and set respectively.

## Installation

```shell
pip install sqlitecollections
```

## Example

```python
import sqlite3
from sqlitecollections import List, Set, Dict

conn = sqlite3.connect("collections.db")

l = List[str](
    connection=conn,
    table_name="list_example",
    data=["Alice", "Bob", "Carol"]
)
print(l[2])
#> Carol
print(len(l))
#> 3
l.append("Dave")
print(l.index("Bob"))
#> 2
d = Dict[str, str](
    connection=conn,
    table_name="dict_example",
    data={"a": "Alice", "b": "Bob"}
)
print(d["a"])
#> Alice
d["c"] = "Carol"
print(list(d.keys()))
#> ['a', 'b', 'c']
print(list(d.values()))
#> ['Alice', 'Bob', 'Carol']
s = Set[str](
    connection=conn,
    table_name="set_example",
    data=["Alice", "Bob", "Carol", "Dave"]
)
print("Ellen" in s)
#> False
print("Alice" in s)
#> True
print(list(s.intersection(["Alice", "Carol"])))
#> ['Alice', 'Carol']
```

The database is updated with each operation, so even if we exit from the python process at this point, the database will still be in the same state and the next time we use the same file, we will be able to use the container from the last time we terminated.

```python
import sqlite3
from sqlitecollections import List

conn = sqlite3.connect("collections.db")

l = List[str](
    connection=conn,
    table_name="list_example",
)
print(len(l))
#> 4
print(l[2])
#> Carol
```

## Pros and cons for built-in containers

### Pros

- Save memory usage.
- Once the database is built, loading time is almost zero, even for huge data.

### Cons

- Each operation has the overhead of serialize/deserialize.
- Some operations are incompatible and unavailable. For example, directly rewriting the mutable elements of a container.
