# ItemsView

`ItemsView` is a view object which is returned by `dict.items()`. It provides a dynamic view on the dictionary's (key, value) tuples, which means when dictionay changes, the view reflects these changes.

---

## `len(itemsview)`

Return the number of items in the dictionary `d: Dict[KT, VT]` where `itemsview=d.items()`.

### Return value:

`int`: The number of items in `d`

---

## `iter(itemsview)`

Return an iterator over the (key, value) tuples in the dictionary.
The order of the items is guaranteed to be inserted order.

### Return value:

`Iterable[Tuple[_KT_co, _VT_co]]`: The iterator over the keys in the dictionary.

---

## `item in itemsview`

Return whether if `item` is in the dictionary.

### Arguments:

- `item`: `object`; An object to be checked

### Return value:

`bool`: `True` if `item` is in the dictionary and `False` otherwise.

---

## `reversed(itemsview)`

Return a reverse iterator over the (key, value) tuples of the dictionary.

(provided only python version 3.8 and above)

### Return value:

`Iterator[Tuple[_KT_co, _VT_co]]`: The reverse iterator over the (key, value) tuples in the dictonary.

---

## `itemsview & other`

Return a set with elements common to `itemsview` and `other`.

### Arguments:

- `other`: `Iterable[Any]`; Iterable to take the intersection.

### Return value:

`Set[Tuple[_KT_co, _VT_co]]`: A set with elements common to `itemsview` and `other`.

---

## `other & itemsview`

Return a set with elements common to `itemsview` and `other`.

### Arguments:

- `other`: `Iterable[_T]`; Iterable to take the intersection.

### Return value:

`Set[_T]`: A set with elements common to `itemsview` and `other`.

---

## `itemsview | other`

Return a new set with elements from `itemsview` and `other`.

### Arguments:

- `other`: `Iterable[_T]`; Iterable to take the union.

### Return value:

`Set[Union[Tuple[_KT_co, _VT_co], _T]]`: A new set with elements from `itemsview` and `other`.

---

## `other | itemsview`

Return a new set with elements from `itemsview` and `other`.

### Arguments:

- `other`: `Iterable[_T]`; Iterable to take the union.

### Return value:

`Set[Union[Tuple[_KT_co, _VT_co], _T]]`: A new set with elements from `itemsview` and `other`.

---

## `itemsview - other`

Return a new set with elements in the `itemsview` that are not in the `other`.

### Arguments:

- `other`: `Iterable[Any]`; Iterable to take the difference.

### Return value:

`Set[Tuple[_KT_co, _VT_co]]`: A new set with elements in the set that are not in the `other`.

---

## `other - itemsview`

Return a new set with elements in the `other` that are not in the `itemsview`.

### Arguments:

- `other`: `Iterable[_T]`; Iterable to take the difference.

### Return value:

`Set[_T]`: A new set with elements in the `other` that are not in the `itemsview`.

---

## `itemsview ^ other`

Return a new set with elements in either the `itemsview` or `other` but not both.

### Arguments:

- `other`: `Iterable[_T]`; Iterable to take the symmetric difference.

### Return value

`Set[Union[Tuple[_KT_co, _VT_co], _T]]`: A new set with elements in either the `itemsview` or `other` but not both.

---

## `other ^ itemsview`

Return a new set with elements in either the `itemsview` or `other` but not both.

### Arguments:

- `other`: `Iterable[_T]`; Iterable to take the symmetric difference.

### Return value

`Set[Union[Tuple[_KT_co, _VT_co], _T]]`: A new set with elements in either the `itemsview` or `other` but not both.

---

## `itemsview.mapping`

Return a `MappingProxyType` that wraps the original dictionary to which the view refers.

(provided only python version 3.10 and above)

### Return value

`MappingProxyType`: A read-only proxy of the original dictionary.

---
