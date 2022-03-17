# ItemsView

`ItemsView` is a view object which is returned by `dict.items()`. It provides a dynamic view on the dictionary's (key, value) tuples, which means when dictionay changes, the view reflects these changes.

---

## `len(itemsview)`

Return the number of items in the dictionary `d: Dict[KT, VT]` where `itemsview=d.items()`.

### Return value:

`int`: The number of items in `d`

---
