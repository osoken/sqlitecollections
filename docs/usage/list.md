# List

`List` is a container compatible with the built-in `list`, which serializes values and stores them in a sqlite3 database.

## `List[T](...)`

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

## `x in s`

`True` if an item of `s` is equal to `x`, else `False`

### Arguments:

- `x`: `object`; an object to be checked

### Return value:

`bool`: `True` if `x` is in `s` and `False` otherwise.

---

## `x not in s`

`False` if an item of `s` is equal to `x`, else `True`

### Arguments:

- `x`: `object`; an object to be checked

### Return value:

`bool`: `False` if `x` is in `s` and `True` otherwise.

---

## `s + t`

The concatenation of `s` and `t`

### Arguments:

- `t`: `Iterable[T]`; an iterable to be concatenated

### Return value:

`List[T]`: the concatenation of `s` and `t`

---

## `s * n` or `n * s`

equivalent to adding `s` to itself `n` times

### Arguments:

- `n`: `int`; number of times to repeat.

### Return value:

`List[T]`: a list of `n` times `s` was repeated

---

## `s[i]`

`i`-th item of `s`, origin `0`

### Arguments:

- `i`: `int`; item index. If it is negative, it is equivalent to `len(s) + i`

### Return value:

`T`: `i`-th item of `s`, origin `0`

---

## `s[i:j]`

Slice of `s` from `i` to `j`

### Arguments:

- `i`: `int` or `None`; item index where the slice starts from. If `None`, it is treated as `0`.
- `j`: `int` or `None`; item index where the slice stops at. If `None`, it is treated as `len(s)`.

### Return value:

`List[T]`: Slice of `s` from `i` to `j`.

---

## `s[i:j:k]`

Slice of `s` from `i` to `j` with step `k`

### Arguments:

- `i`: `int` or `None`; item index where the slice starts from. If `None`, it is treated as `0`.
- `j`: `int` or `None`; item index where the slice stops at. If `None`, it is treated as `len(s)`.
- `k`: `int` or `None`; step of the slice. If `None`, it is treated as `1`.

### Return value:

`List[T]`: Slice of `s` from `i` to `j` with step `k`.

---

## `len(s)`

Return the number of items in `s: List[T]`

### Return value:

`int`: The number of items in `s`

---
