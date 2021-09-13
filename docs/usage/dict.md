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

### Arguments:

- `key`: `KT`; Return the item of `d` with key `key`. Raises a `KeyError` if `key` is not in the map

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
