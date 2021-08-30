
CREATE TABLE metadata (
    table_name TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    container_type TEXT NOT NULL,
    UNIQUE (table_name, container_type)
);

INSERT INTO metadata (
    table_name, schema_version, container_type
) VALUES (
    "items", "0", "Dict"
);

CREATE TABLE items (
    serialized_key BLOB NOT NULL UNIQUE, 
    serialized_value BLOB NOT NULL, 
    item_order INTEGER PRIMARY KEY
);
