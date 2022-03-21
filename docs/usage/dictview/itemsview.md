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
