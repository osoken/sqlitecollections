
CREATE TABLE metadata (
    table_name TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    container_type TEXT NOT NULL,
    UNIQUE (table_name, container_type)
);

INSERT INTO metadata (
    table_name, schema_version, container_type
) VALUES (
    "items", "0", "Set"
);

CREATE TABLE items (
    serialized_value BLOB PRIMARY KEY
);
