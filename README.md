# Databricks SQL

Databricks SQL framework, easy to learn, fast to code, ready for production.

## Installation

```shell
$ pip install databricks-sql
```

## Configuration

```python
from databricks_sql import Configuration

CONFIGURATION = Configuration.instance(
    access_token="",
    command_directory="",
    http_path="",
    server_hostname="",
)
```

## License

This project is licensed under the terms of the [Apache License 2.0](https://github.com/bernardocouto/databricks-sql/blob/main/LICENSE).
