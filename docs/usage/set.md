# Set

`Set` is a container compatible with the built-in `set`, which serializes values and stores them in a sqlite3 database.

## `Set[T](...)`

Constructor.

### Type Parameters:

- `T`: value type

### Arguments:

- `connection`: `str` or `sqlite3.Connection`, optional, default=`None`; If `None`, temporary file is automatically created. If `connection` is a `str`, it will be used as the sqlite3 database file name. You can pass a `sqlite3.Connection` directly.
- `table_name`: `str`, optional, default=`None`; Table name of this container. If `None`, an auto-generated unique name will be used. Available characters are letters, numbers, and underscores (`_`).
- `serializer`: `Callable[[T], bytes]`, optional, default=`None`; Function to serialize value. If `None`, `pickle.dumps` is used.
- `deserializer`: `Callable[[bytes], T]`, optional, default=`None`; Function to deserialize value. If `None`, `pickle.loads` is used.
- `persist`: `bool`, optional, default=`True`; If `True`, table won't be deleted even when the object is deleted. If `False`, the table is deleted when this object is deleted.
- `rebuild_strategy`: `RebuildStrategy`, optional, default=`RebuildStrategy.CHECK_WITH_FIRST_ELEMENT`; Rebuild strategy.
- `data`: `Iterable[T]`, optional, defualt=`None`; Initial data.

---

## `len(s)`

Return the number of items in `s: Set[T]`

### Return value:

`int`: The number of items in `s`

---

## `x in s`

`True` if `x` is in `s`, else `False`

### Arguments:

- `x`: `object`; an object to be checked

### Return value:

`bool`: `True` if `x` is in `s` and `False` otherwise.

---

## `x not in s`

`False` if `x` is in `s`, else `True`

### Arguments:

- `x`: `object`; an object to be checked

### Return value:

`bool`: `False` if `x` is in `s` and `True` otherwise.

---

## `isdisjoint(other)`

Return `True` if the set has no elements in common with `other`.

### Arguments:

- `other`: `Iterable[T]`; Iterable to check if disjoint.

### Return value:

`bool`: `True` if the set is disjoint with `other`, `False` otherwise.

---

## `issubset(other)`

Return `True` if every element in the set is in `other`.

### Arguments:

- `other`: `Iterable[T]`; Iterable to check if this one contains the set.

### Return value:

`bool`: `True` if the set is subset of `other`, `False` otherwise.

---

## `s <= other`

Return `True` if every element in the set is in `other`.

### Argument:

- `other` : `Iterable[T]`; Iterable to check if this one contains all the elements in the set.

### Return value:

`bool`: `True` if the set is subset of `other`, `False` otherwise.

---

## `s < other`

Return `True` if every element in the set is in `other` and `s` is not equal to `other`.

### Arguments:

- `other`: `Iterable[T]`; Iterable to check if it contains all elements of the set and is not equal to the set.

### Return value:

`bool`: `True` if the set is a proper subset of `other`, `False` otherwise.

---

## `issuperset(other)`

Return `True` if every element in `other` is in the set.

### Arguments:

- `other`: `Iterable[T]`; Iterable to check if the elements are contained.

### Return value:

`bool`: `True` if the set is superset of `other`, `False` otherwise.

---

## `s >= other`

Return `True` if every element in `other` is in the set.

### Argument:

- `other`: `Iterable[T]`; Iterable to check if the elements are contained.

### Return value:

`bool`: `True` if the set is superset of `other`, `False` otherwise.

---

## `s > other`

Return `True` if every element in `other` is in the set and `s` is not equal to `other`.

### Arguments:

- `other`: `Iterable[T]`; Iterable to check if it all elements are in the set and is not equal to the set.

### Return value:

`bool`: `True` if the set is a proper superset of `other`, `False` otherwise.

---

## `union(*others)`

Return a new set with elements from the set and all `others`.

### Arguments:

- `others`: `Iterable[T]`; Iterables to take the union.

### Return value:

`Set[T]`: A new set with elements from the set and all `others`.

---

## `s | other`

Return a new set with elements from `s` and `other`.

### Arguments:

- `other`: `Iterable[T]`; Iterable to take the union.

### Return value:

`Set[T]`: A new set with elements from `s` and `other`.

---

## `intersection(*others)`

Return a new set with elements common to the set and all `others`.

### Arguments:

- `others`: `Iterable[T]`; Iterables to take the intersection.

### Return value:

`Set[T]`: A new set with elements common to the set and all `others`.

---

## `s & other`

Return a new set with elements common to `s` and `other`.

### Arguments:

- `other`: `Iterable[T]`; Iterable to take the intersection.

### Return value:

`Set[T]`: A new set with elements commont ot `s` and `other`.

---
