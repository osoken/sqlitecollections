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

## `keysview - other`

Return a new set with elements in the `keysview` that are not in the `other`.

### Arguments:

- `other`: `Iterable[Any]`; Iterable to take the difference.

### Return value:

`Set[_KT_co]`: A new set with elements in the set that are not in the `other`.

---

## `other - keysview`

Return a new set with elements in the `other` that are not in the `keysview`.

### Arguments:

- `other`: `Iterable[_T]`; Iterable to be taken the difference.

### Return value:

`Set[_T]`: A new set with elements in `other` that are not in the `keysview`.

---

## `keysview ^ other`

Return a new set with elements in either the `keysview` or `other` but not both.

### Arguments:

- `other`: `Iterable[_T]`; Iterable to take the symmetric difference.

### Return value

`Set[Union[_KT_co, _T]]`: A new set with elements in either the `keysview` or `other` but not both.

---

## `other ^ keysview`

Return a new set with elements in either the `keysview` or `other` but not both.

### Arguments:

- `other`: `Iterable[_T]`; Iterable to take the symmetric difference.

### Return value

`Set[Union[_KT_co, _T]]`: A new set with elements in either the `keysview` or `other` but not both.

---
