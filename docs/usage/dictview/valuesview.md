# ValuesView

`ValuesView` is a view object which is returned by `dict.views()`. It provides a dynamic view on the dictionary's values, which means when dictionay changes, the view reflects these changes.

---

## `len(valuesview)`

Return the number of items in the dictionay `d: Dict[KT, VT]` where `valuesview=d.values()`.

### Return value:

`int`: The number of items in `d`

---
