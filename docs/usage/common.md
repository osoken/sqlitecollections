# Common features

# PicklingStrategy

`PicklingStrategy` specifies the way how pickle serializes the sqlitecollection containers.
Currently following two methods are supported:

## `PicklingStrategy.whole_table`

The resulting pickle contains all the records in the collection.
It dumps all the records into temporary sqlite3 database file when it is loaded.

## `PicklingStrategy.only_file_name`

The resulting pickle contains only the file path of the sqlite3 database file.
The file path is a relative path returned by `os.path.relpath`, so it must be loaded with the same structure.

# MetadataItem

## `MetadataItem(...)`

Constructor for MetadataItem.
Instances of this class are not supposed to be created directly by the user.

### Arguments:

- `table_name`: `str`; table name.
- `container_type`: `str`; container type name.
- `schema_version`: `str`; schema version.

---

## `table_name`

Read-only property for the table name.

### Return value:

`str`: The table name.

---

## `container_type`

Read-only property for the container type.

### Return value:

`str`: The container type.

---

## `schema_version`

Read-only property for the schema version

### Return value:

`str`: The schema version.

---

# MetadataReader

## `MetadataReader(...)`

Constructor for `MetadataReader` class which can iterate over all records in the given connection.

### Arguments:

- `connection`: `str` or `sqlite3.Connection`; Connection to sqlite3 to get metadata. If `connection` is a `str`, it will be used as the sqlite3 database file name. You can pass a `sqlite3.Connection` directly.

---

## `len(metadata_reader)`

Return the number of records in the metadata table read by `metadata_reader`.
It coincides the number of collections in the sqlite3 database connected by `metadata_reader._connection`.

### Return value:

`int`: The number of items in the metadata table.

---

## `obj in metadata_reader`

Return whether if `obj` is in `metadata_reader`

### Arguments:

- `obj`: `object`; object to be checked the membership.

### Return value:

`bool`: Boolean value whether if `metadata_item` is in the metadata table.

---

## `iter(metadata_reader)`

Return an iterator over the `MetadataItem`'s in `metadata_reader`
It coincides the records of metadata table in the sqlite3 database connected by `metadata_reader._connection`.

### Return value:

`Iterator[MetadataItem]`: an iterator over the `MetadataItem`'s in `metadata_reader`
