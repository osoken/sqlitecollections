# Dict

`Dict` is a container compatible with the built-in `dict`, which serializes keys and values and stores them in a sqlite3 database.
It preserves insertion order for all supported python versions.

## `Dict[KT, VT](...)`

Constructor.

### Type Parameters:

- `KT`: key type
- `VT`: value type

### Arguments:

- `connection`: `str` or `sqlite3.Connection`, optional, default=`None`; If `None`, temporary file is automatically created. If `connection` is a `str`, it will be used as the sqlite3 database file name. You can pass a `sqlite3.Connection` directly.
- `table_name`: `str`, optional, default=`None`; Table name of this container. If `None`, an auto-generated unique name will be used. Available characters are letters, numbers, and underscores (`_`).
- `serializer`: `Callable[[VT], bytes]`, optional, default=`None`; Function to serialize value. If `None`, `pickle.dumps` is used.
- `deserializer`: `Callable[[bytes], VT]`, optional, default=`None`; Function to deserialize value. If `None`, `pickle.loads` is used.
- `key_serializer`: `Callable[[KT], bytes]`, optional, default=`None`; Function to serialize key. If `None`, `serializer` is used.
- `key_deserializer`: `Callable[[bytes], KT]`, optional, default=`None`; Function to deserialize key. If `None`, `deserializer` is used.
- `persist`: `bool`, optional, default=`True`; If `True`, table won't be deleted even when the object is deleted. If `False`, the table is deleted when this object is deleted.
- `rebuild_strategy`: `RebuildStrategy`, optional, default=`RebuildStrategy.CHECK_WITH_FIRST_ELEMENT`; Rebuild strategy.
- `data`: `Mapping[KT, VT]`, optional, defualt=`None`; Initial data.

---

## `len(d)`

Return the number of items in `d: Dict[KT, VT]`

### Return value:

`int`: The number of items in `d`

---

## `d[key]`

Get item of `d` with key `key`.
Raises a `KeyError` if `key` is not in the map.

### Arguments:

- `key`: `KT`; Key to retrieve corresponding value

### Return value:

`VT`: Item of `d` with key `key`

---

## `d[key] = value`

Set `d[key]` to `value`.

### Arguments:

- `key`: `KT`; Key to be set
- `value`: `VT`; Value to be set

### Return value:

`None`

---

## `del d[key]`

Delete `d[key]`.

### Arguments:

- `key`: `KT`; Key to be deleted

### Return value:

`None`

---

## `key in d`

Return whether if `key` is in `d`

### Arguments:

- `key`: `KT`; Key to be checked

### Return value:

`bool`: `True` if `key` is in `d` and `False` otherwise.

---

## `key not in d`

Return whether if `key` is not in `d`

### Arguments:

- `key`: `KT`; Key to be checked

### Return value:

`bool`: `True` if `key` is not in `d` and `False` otherwise.

---

## `iter(d)`

Return an iterator over the keys of `d`

### Return value:

`Iterator[KT]`: an iterator over the keys of `d`

---

## `clear()`

Remove all items from the dictionary

### Return value:

`None`

---

## `copy()`

Return a copy of the dictionary.
The actual behavior is to create a table with a unique table name and copy the keys and the values to the new table.
Therefore, unlike the built-in dict copy, the behavior is similar to deep copy.
Be aware that the copied dictionary is volatile.

### Return value:

`Dict[KT, VT]`: A volatile copy of the dictionary.

---

## `get(key[, default])`

Return the value for `key` if `key` is in the dictionary, else `default`. If `default` is not given, it defaults to `None`, so that this method never raises a `KeyError`.

### Arguments:

- `key`: `KT`; Key to retrieve corresponding value if exists.
- `default`: `VT`, optional, default=`None`; Default value in case that `key` is not in the dictionary

### Return value:

`VT`: Item of the dictionary with key `key` if `key` is in the dictionary, `default` otherwise.

---

## `items()`

Return a new view of the dictionary’s items (key-value pairs).

### Return value:

`ItemsView`: View object of the dictionary's items

---

## `keys()`

Return a new view of the dictionary's keys.

### Return value:

`KeysView`: View object of the dictionary's keys

---

## `pop(key[, default])`

If `key` is in the dictionary, remove it and return its value, else return `default`. If `default` is not given and `key` is not in the dictionary, a `KeyError` is raised.

### Arguments:

- `key`: `KT`; Key to retrieve corresponding value if exists.
- `default`: `VT`, optional, default=`None`; Default value in case that `key` is not in the dictionary

### Return value:

`VT`: Item of the dictionary with key `key` if `key` is in the dictionary, `default` otherwise.

---

## `popitem()`

Remove and return a key-value pair from the dictionary. Pairs are returned in LIFO order.
If the dictionary is empty, raises a `KeyError`.

### Return value:

`Tuple[KT, VT]`: Key-value pair that were last inserted into the dictionary

---
