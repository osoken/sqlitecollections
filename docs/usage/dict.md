# Dict

`Dict` is a container compatible with the built-in `dict`, which serializes keys and values and stores them in a sqlite3 database.
It preserves insertion order for all supported python versions.

## `Dict[KT, VT](...)`

Constructor.

### Type Parameters:

- `KT`: key type
- `VT`: value type

### Arguments:

- `data`: `Mapping[KT, VT]` or `Iterable[Tuple[KT, VT]]`, optional, positional-only argument, defualt=`None`; Initial data.
- `connection`: `str` or `sqlite3.Connection`, optional, default=`None`; If `None`, temporary file is automatically created. If `connection` is a `str`, it will be used as the sqlite3 database file name. You can pass a `sqlite3.Connection` directly.
- `table_name`: `str`, optional, default=`None`; Table name of this container. If `None`, an auto-generated unique name will be used. Available characters are letters, numbers, and underscores (`_`).
- `key_serializer`: `Callable[[KT], bytes]`, optional, default=`None`; Function to serialize key. If `None`, `pickle.dumps` is used.
- `key_deserializer`: `Callable[[bytes], KT]`, optional, default=`None`; Function to deserialize key. If `None`, `pickle.loads` is used.
- `value_serializer`: `Callable[[VT], bytes]`, optional, default=`None`; Function to serialize value. If `None`, `key_serializer` is used.
- `value_deserializer`: `Callable[[bytes], VT]`, optional, default=`None`; Function to deserialize value. If `None`, `key_deserializer` is used.
- `persist`: `bool`, optional, default=`True`; If `True`, table won't be deleted even when the object is deleted. If `False`, the table is deleted when this object is deleted.
- `rebuild_strategy`: `RebuildStrategy`, optional, default=`RebuildStrategy.CHECK_WITH_FIRST_ELEMENT`; Rebuild strategy.

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

`KeysView[KT]`: View object of the dictionary's keys.

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

## `reversed(d)`

Return a reverse iterator over the keys of the dictionary.

(provided only python version 3.8 and above)

### Return value:

`Iterator[KT]`: Reverse iterator over the keys of the dictionary

---

## `setdefault(key[, default])`

If `key` is in the dictionary, return its value. If not, insert `key` with a value of `default` and return `default`.

### Arguments:

- `key`: `KT`; Key to retrieve or set the value
- `default`: `VT`, optional, default=`None`; Value to be set if `key` is not in the dictionary

### Return value:

`VT`: Item of the dictionary with key `key` if `key` is in the dictionary, `default` otherwise.

---

## `update([other, **kwargs])`

Update the dictionary with the key-value pairs from `other`, overwriting existing keys.

### Arguments:

- `other`: `Mapping[KT, VT]` or `Iterable[Tuple[KT, VT]]`, optional; Key-value pairs to be added
- `kwargs`: `VT`, optional; values to be added

### Return value:

`None`

---

## `values()`

Return a new view of the dictionary's values.

### Return value:

`ValuesView`: View object of the dictionary's values

---

## `d | other`

Create a new dictionary with the merged keys and values of `d` and `other`, which must both be dictionaries.
The values of `other` take priority when `d` and `other` share keys.
The return value is volatile by default.

(provided only python version 3.9 and above)

### Arguments:

- `other`: `Mapping[KT, VT]` or `Iterable[Tuple[KT, VT]]`; Key-value pairs to be merged

### Return value:

`Dict[KT, VT]`: A new volatile dictionary object.

---

## `d |= other`

Update the dictionary `d` with keys and values from `other`.

(provided only python version 3.9 and above)

### Arguments:

- `other`: `Mapping[KT, VT]` or `Iterable[Tuple[KT, VT]]`; Key-value pairs to be merged

### Return value:

`Dict[KT, VT]`: The dictionary object.

===

# KeysView

`KeysView` is a view object which is returned by `dict.keys()`. It provides a dynamic view on the dictionary's keys, which means when dictionay changes, the view reflects these changes.

---

## `len(keysview)`

Return the number of items in the dictionary `d: Dict[KT, VT]` where `keysview=d.keys()`.

### Return value:

`int`: The number of items in `d`

---

## `iter(keysview)`

Return an iterator over the keys in the dictionary.
The order of the keys is guaranteed to be inserted order.

### Return value:

`Iterator[_KT_co]`: The iterator over the keys in the dictonary.

---

## `key in keysview`

Return whether if `key` is in the dictionary.

### Arguments:

- `key`: `object`; An object to be checked

### Return value:

`bool`: `True` if `key` is in the dictionary and `False` otherwise.

---

## `reversed(keysview)`

Return a reverse iterator over the keys of the dictionary.

(provided only python version 3.8 and above)

### Return value:

`Iterator[_KT_co]`: The reverse iterator over the keys in the dictonary.

---

## `keysview & other`

Return a set with elements common to `keysview` and `other`.

### Arguments:

- `other`: `Iterable[Any]`; Iterable to take the intersection.

### Return value:

`Set[_KT_co]`: A set with elements common to `keysview` and `other`.

---

## `other & keysview`

Return a set with elements common to `keysview` and `other`.

### Arguments:

- `other`: `Iterable[_T]`; Iterable to take the intersection.

### Return value:

`Set[_T]`: A set with elements common to `keysview` and `other`.

---

## `keysview | other`

Return a new set with elements from `keysview` and `other`.

### Arguments:

- `other`: `Iterable[_T]`; Iterable to take the union.

### Return value:

`Set[Union[_KT_co, _T]]`: A new set with elements from `keysview` and `other`.

---

## `other | keysview`

Return a new set with elements from `keysview` and `other`.

### Arguments:

- `other`: `Iterable[_T]`; Iterable to take the union.

### Return value:

`Set[Union[_KT_co, _T]]`: A new set with elements from `keysview` and `other`.

---
