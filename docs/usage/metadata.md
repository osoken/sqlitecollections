# Metadata

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

## `MetadataReader(...)`

Constructor for `MetadataReader` class which can iterate over all records in the given connection.

### Arguments:

- `connection`: `str` or `sqlite3.Connection`; Connection to sqlite3 to get metadata. If `connection` is a `str`, it will be used as the sqlite3 database file name. You can pass a `sqlite3.Connection` directly.
