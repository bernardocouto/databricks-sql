# Databricks SQL

Databricks SQL framework, easy to learn, fast to code, ready for production.

## Installation

```shell
$ pip install databricks-sql
```

## Configuration

```python
from databricks_sql.client import Configuration

CONFIGURATION = Configuration.instance(
    access_token="",
    command_directory="",
    http_path="",
    server_hostname="",
)
```

## Usage

Databricks SQL usage description:

### Delete

#### Delete with where

```python
from databricks_sql.client import Database

with Database() as connection:
    (
        connection
        .delete("catalog.schema.table")
        .where("id", "994238a4-8c18-436a-8c06-29ec89c4c056")
        .execute()
    )
```

#### Delete with where condition

```python
from databricks_sql.client import Database

with Database() as connection:
    (
        connection
        .delete("catalog.schema.table")
        .where("description", "%Databricks%", operator="LIKE")
        .execute()
    )
```

### Execute

```python
from databricks_sql.client import Database

with Database() as connection:
    (
        connection
        .execute(
            """
                CREATE TABLE IF NOT EXISTS catalog.schema.table (
                    id STRING NOT NULL,
                    name STRING NOT NULL,
                    description STRING,
                    CONSTRAINT table_primary_key PRIMARY KEY(id)
                ) USING DELTA
            """,
            parameters=None,
            skip_load=True,
        )
    )
```

### Insert

```python
from databricks_sql.client import Database

with Database() as connection:
    (
        connection
        .insert("catalog.schema.table")
        .set("id", "994238a4-8c18-436a-8c06-29ec89c4c056")
        .set("name", "Name")
        .set("description", "Description")
        .execute()
    )
```

### Paging

#### Paging with where condition

```python
from databricks_sql.client import Database

with Database() as connection:
    (
        connection
        .select("catalog.schema.table")
        .fields("id", "name", "description")
        .where("name", "%Databricks%", operator="LIKE")
        .order_by("id")
        .paging(0, 10)
    )
```

#### Paging without where condition

```python
from databricks_sql.client import Database

with Database() as connection:
    (
        connection
        .select("catalog.schema.table")
        .paging(0, 10)
    )
```

### Select

#### Fetch all

```python
from databricks_sql.client import Database

with Database() as connection:
    (
        connection
        .select("catalog.schema.table")
        .execute()
        .fetch_all()
    )
```

#### Fetch many

```python
from databricks_sql.client import Database

with Database() as connection:
    (
        connection
        .select("catalog.schema.table")
        .execute()
        .fetch_many(1)
    )
```

#### Fetch one

```python
from databricks_sql.client import Database

with Database() as connection:
    (
        connection
        .select("catalog.schema.table")
        .execute()
        .fetch_one()
    )
```

#### Select by file

```python
from databricks_sql.client import Database

with Database() as connection:
    (
        connection
        .execute("command.sql", {"id": "994238a4-8c18-436a-8c06-29ec89c4c056"})
        .fetch_one()
    )
```

#### Select by command

```python
from databricks_sql.client import Database

with Database() as connection:
    (
        connection
        .execute("SELECT id, name, description FROM catalog.schema.table WHERE id = %(id)s", {"id": "994238a4-8c18-436a-8c06-29ec89c4c056"})
        .fetch_one()
    )
```

### Update

#### Update with where

```python
from databricks_sql.client import Database

with Database() as connection:
    (
        connection
        .update("catalog.schema.table")
        .set("name", "New Name")
        .set("description", "New Description")
        .where("id", "994238a4-8c18-436a-8c06-29ec89c4c056")
        .execute()
    )
```

#### Update with where all

```python
from databricks_sql.client import Database

with Database() as connection:
    (
        connection
        .update("catalog.schema.table")
        .set("name", "New Name")
        .set("description", "New Description")
        .where_all({"id": "994238a4-8c18-436a-8c06-29ec89c4c056", "name": "Name", "description": "Description"})
        .execute()
    )
```

### Using mustache

#### SQL

```sql
select
    id,
    name,
    description
from catalog.schema.table
where 1 = 1
{{#id}}
and id = %(id)s
{{/id}}
{{#name}}
and name like %(name)s
{{/name}}
```

#### Select with filters

```python
from databricks_sql.client import Database

with Database() as connection:
    (
        connection
        .execute("command.sql", parameters={"id": "994238a4-8c18-436a-8c06-29ec89c4c056", "name": "Name"})
        .fetch_one()
    )
```

## License

This project is licensed under the terms of the [Apache License 2.0](https://github.com/bernardocouto/databricks-sql/blob/main/LICENSE).
