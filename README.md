# sqlitecollections

`sqlitecollections` is a sort of containers that are backended by sqlite3 DB and are compatible with corresponding built-in collections. Since containers consume disk space instead of RAM, they can handle large amounts of data even in environments with limited RAM. Migrating from existing code using the built-in container is as simple as importing the library and changing the constructor.

The elements of the container are automatically serialized and stored in the sqlite3 database, and are automatically read from the sqlite3 database and deserialized when accessed. Current version supports List (mutable sequence), Dict (mutable mapping) and Set (mutable set) and almost all methods are compatible with list, dict and set respectively.

## Installation

```shell
pip install sqlitecollections
```

## Example

```python
import sqlitecollections as sc

l = sc.List[str](data=["Alice", "Bob", "Carol"])
print(l[2])
#> Carol
print(len(l))
#> 3
l.append("Dave")
print(l.index("Bob"))
#> 1
print(l.index("Dave"))
#> 3

d = sc.Dict[str, str](data={"a": "Alice", "b": "Bob"})
print(d["a"])
#> Alice
d["c"] = "Carol"
print(list(d.keys()))
#> ['a', 'b', 'c']
print(list(d.values()))
#> ['Alice', 'Bob', 'Carol']

s = sc.Set[str](data=["Alice", "Bob", "Carol", "Dave"])
print("Ellen" in s)
#> False
print("Alice" in s)
#> True
print(list(s.intersection(["Alice", "Carol"])))
#> ['Alice', 'Carol']
```

In the above example, a temporary file is created every time a container is created, and the elements are written to the sqlite3 database created on the file, thus consuming very little RAM.

If you want to reuse the container you created, you can create it by specifying the file path and table name of the sqlite3 database.

```python
import sqlitecollections as sc

l = sc.List[str](connection="path/to/file.db", table_name="list_example", data=["Alice", "Bob", "Carol"])
l.append("Dave")
exit()
```

When you load it, you can restore the previous state by specifying the same file path and table name.

```python
import sqlitecollections as sc

l = sc.List[str](connection="path/to/file.db", table_name="list_example")
print(len(l))
#> 4
print(list(l))
#> ['Alice', 'Bob', 'Carol', 'Dave']
```

## Pros and cons for built-in containers

### Pros

- Save memory usage.
- Once the database is built, loading time is almost zero, even for huge data.

### Cons

- Each operation has the overhead of serialize/deserialize.
- Some operations are incompatible and unavailable. For example, directly rewriting the mutable elements of a container.
