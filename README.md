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
        .select("test")
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
        .select("test")
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
        .select("test")
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
        .execute("read_by_id", {"id": "994238a4-8c18-436a-8c06-29ec89c4c056"})
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

## License

This project is licensed under the terms of the [Apache License 2.0](https://github.com/bernardocouto/databricks-sql/blob/main/LICENSE).
