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
