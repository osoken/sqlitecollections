# ValuesView

`ValuesView` is a view object which is returned by `dict.views()`. It provides a dynamic view on the dictionary's values, which means when dictionay changes, the view reflects these changes.

---

## `len(valuesview)`

Return the number of items in the dictionay `d: Dict[KT, VT]` where `valuesview=d.values()`.

### Return value:

`int`: The number of items in `d`

---

## `iter(valuesview)`

Return an iterator over the values in the dictionary.
The order of the values is guaranteed to be inserted order.

### Return value:

`Iterator[_VT_co]`: The iterator over the values in the dictionary.

---

## `value in valuesview`

Return whether if `value` is in the dictionary.

### Arguments:

- `value`: `object`; An object to be checked

### Return value:

`bool`: `True` if `value` is in the dictionary and `False` otherwise.

---

## `reversed(valuesview)`

Return a reverse iterator over the values of the dictionary.

(provided only python version 3.8 and above)

### Return value:

`Iterator[_VT_co]`: The reverse iterator over the values in the dictonary.

---

## `valuesview.mapping`

Return a `MappingProxyType` that wraps the original dictionary to which the view refers.

(provided only python version 3.10 and above)

### Return value

`MappingProxyType`: A read-only proxy of the original dictionary.

---
