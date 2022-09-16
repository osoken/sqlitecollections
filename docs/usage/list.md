# List

`List` is a container compatible with the built-in `list`, which serializes values and stores them in a sqlite3 database.

## `List[T](...)`

Constructor.

### Type Parameters:

- `T`: value type

### Arguments:

- `data`: `Iterable[T]`, optional, positional-only argument, defualt=`None`; Initial data. If `None` or no argument is given, persistent data is used as is if available, otherwise persistent data in the corresponding table is cleared and given data is stored instead.
- `connection`: `str` or `sqlite3.Connection`, optional, default=`None`; If `None`, temporary file is automatically created. If `connection` is a `str`, it will be used as the sqlite3 database file name. You can pass a `sqlite3.Connection` directly.
- `table_name`: `str`, optional, default=`None`; Table name of this container. If `None`, an auto-generated unique name will be used. Available characters are letters, numbers, and underscores (`_`).
- `serializer`: `Callable[[T], bytes]`, optional, default=`None`; Function to serialize value. If `None`, `pickle.dumps` is used.
- `deserializer`: `Callable[[bytes], T]`, optional, default=`None`; Function to deserialize value. If `None`, `pickle.loads` is used.
- `persist`: `bool`, optional, default=`True`; If `True`, table won't be deleted even when the object is deleted. If `False`, the table is deleted when this object is deleted.
- `pickling_strategy`: `PicklingStrategy`, optional, default=`PicklingStrategy.whole_table`; Flag to control pickling method. See [`PicklingStrategy`](common.md#picklingstrategy) for more details.

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

- `t`: `Iterable[T]`; An iterable to be concatenated.

### Return value:

`List[T]`: The concatenation of `s` and `t`.

---

## `s * n` or `n * s`

equivalent to adding `s` to itself `n` times.

### Arguments:

- `n`: `int`; Number of times to repeat.

### Return value:

`List[T]`: a list of `n` times `s` was repeated.

---

## `s[i]`

`i`-th item of `s`, origin `0`.

### Arguments:

- `i`: `int`; Item index. If it is negative, it is equivalent to `len(s) + i`.

### Return value:

`T`: `i`-th item of `s`, origin `0`.

---

## `s[i:j]`

Slice of `s` from `i` to `j`.

### Arguments:

- `i`: `int` or `None`; Item index where the slice starts from. If `None`, it is treated as `0`.
- `j`: `int` or `None`; Item index where the slice stops at. If `None`, it is treated as `len(s)`.

### Return value:

`List[T]`: Slice of `s` from `i` to `j`.

---

## `s[i:j:k]`

Slice of `s` from `i` to `j` with step `k`

### Arguments:

- `i`: `int` or `None`; Item index where the slice starts from. If `None`, it is treated as `0`.
- `j`: `int` or `None`; Item index where the slice stops at. If `None`, it is treated as `len(s)`.
- `k`: `int` or `None`; Step of the slice. If `None`, it is treated as `1`.

### Return value:

`List[T]`: Slice of `s` from `i` to `j` with step `k`.

---

## `len(s)`

Return the number of items in `s: List[T]`

### Return value:

`int`: The number of items in `s`

---

## `index(x[, i[, j]]])`

Return index of the first occurrence of `x` in the list (at or after index `i` and before index `j`).
Raise `ValueError` if `x` is not in the list.

### Arguments:

- `x`: `T`; Value to retrieve corresponding index if exists.
- `i`: `int`, optional, default=`None`; First index to look up the value. If it is `None`, start from the first element of the list.
- `j`: `int`, optional, default=`None`; Index before the last one to look for a value. If `None`, search to the end.

### Return value:

`int`: Index of the first occurrence of `x` in the list.

---

## `count(x)`

Return total number of occurrences of `x` in the list.

### Arguments:

- `x`: `T`; Value that counts the number of occurrences.

### Return value:

`int`: The number of occurrences of `x` in the list.

---

## `s[i] = x`

Replace item `i` of `s` by `x`.

### Arguments:

- `i`: `int`; Item index to be set. If it is negative, it is equivalent to `len(s) + i`
- `x`: `T`; Value to be set.

### Return value:

`None`.

---

## `del s[i]`

Remove item `i` of `s`.

### Arguments:

- `i`: `int`; Item index to be removed. If it is negative, it is equivalent to `len(s) + i`.

### Return value:

`None`.

---

## `s[i:j] = t`

Replace slice of `s` from `i` to `j` by the contents of the iterable `t`.

### Arguments:

- `i`: `int` or `None`; Item index where the slice starts from. If `None`, it is treated as `0`.
- `j`: `int` or `None`; Item index where the slice stops at. If `None`, it is treated as `len(s)`.
- `t`: `Iterable[T]`; Iterable to be inserted.

### Return value:

`None`.

---

## `del s[i:j]`

Remove slice of `s` from `i` to `j`.

### Arguments:

- `i`: `int` or `None`; Item index where the slice starts from. If `None`, it is treated as `0`.
- `j`: `int` or `None`; Item index where the slice stops at. If `None`, it is treated as `len(s)`.

### Return value:

`None`.

---

## `s[i:j:k] = t`

Replace the elements of `s[i:j:k]` by those ofthe iterable `t`. The length must be the same. Otherwise, `ValueError` will be raised.

### Arguments:

- `i`: `int` or `None`; Item index where the slice starts from. If `None`, it is treated as `0`.
- `j`: `int` or `None`; Item index where the slice stops at. If `None`, it is treated as `len(s)`.
- `k`: `int` or `None`; Step of the slice. If `None`, it is treated as `1`.
- `t`: `Iterable[T]`; Iterable to be substituted. The length must be the same as that of the slice.

### Return value:

`None`.

---

## `del s[i:j:k]`

Remove the elements of `s[i:j:k]` from the list.

### Arguments:

- `i`: `int` or `None`; Item index where the slice starts from. If `None`, it is treated as `0`.
- `j`: `int` or `None`; Item index where the slice stops at. If `None`, it is treated as `len(s)`.
- `k`: `int` or `None`; Step of the slice. If `None`, it is treated as `1`.

### Return value:

`None`.

---

## `append(x)`

Append `x` to the end of the sequence.

### Arguments:

- `x`: `T`; Item to be appended.

### Return value;

`None`.

---

## `clear()`

Remove all items from the list.

### Return value:

`None`.

---

## `copy()`

Return a copy of the list. The actual behavior is to create a table with a unique table name and copy the items to the new table. Therefore, unlike the built-in list copy, the behavior is similar to deep copy. Be aware that the copied list is volatile.

### Return value:

`List[T]`: A volatile copy of the list.

---

## `extend(t)`

Concatenate the list and `t`.

### Arguments:

- `t`: `Iterable[T]`; An iterable to be concatenated.

### Return value:

`None`.

---

## `s += t`

Concatenate `s` and `t`.

### Arguments:

- `t`: `Iterable[T]`; An iterable to be concatenated.

### Return value:

`None`.

---

## `s *= n`

Update `s` with its contents repeated `n` times.

### Arguments:

- `n`: `int`; Number of times to repeat.

### Return value:

`None`.

---

## `insert(i, x)`

Insert `x` into the list at the index `i`.

### Arguments:

- `i`: `int`; Index to be inserted.
- `x`: `T`; Item to be inserted.

### Return value:

`None`.

---

## `pop(i)`

Retrieve the item at `i` and also remove it from s.

### Arguments:

- `i`: `int` or `None`, optional, default=`None`; Index to be retrieved and removed. If it is `None`, it is treated as the last index of the list.

### Return value:

`T`: `i`-th item of `s`, origin `0`.

---

## `remove(x)`

Remove the first item of `s` whose value is equal to `x`.

### Arguments:

- `x`: `T`; Item to be removed.

### Return value:

`None`.

---

## `reverse()`

Reverse the items of the list in place.

### Return value:

`None`.

---

## `sort(reverse, key)`

Sort the items of the list in place. The value of `reverse` can be either `True` or `False`, resulting in descending or ascending order, respectively. `key` specifies a function of one argument that is used to extract a comparison key from each list element.

### Arguments:

- `reverse`: `bool`, optional, default=`False`; By default, the order is ascending, but if this value is `True`, the order will be descending.
- `key`: `Callable[[T], Any]`, optional, default=`None`; Function to extract a comparison key from each list element.

### Return value:

`None`.
