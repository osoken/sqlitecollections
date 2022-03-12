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
