from databricks import sql
from typing import Any

import errno
import pystache


class Configuration(object):

    __instance__ = None

    def __init__(
        self,
        access_token: str,
        command_directory: str,
        http_path: str,
        server_hostname: str,
    ):
        self.access_token = access_token
        self.command_directory = command_directory
        self.http_path = http_path
        self.server_hostname = server_hostname

    @staticmethod
    def instance(
        access_token: str, command_directory: str, http_path: str, server_hostname: str
    ):
        if Configuration.__instance__ is None:
            Configuration.__instance__ = Configuration(
                access_token=access_token,
                command_directory=command_directory,
                http_path=http_path,
                server_hostname=server_hostname,
            )
        return Configuration.__instance__


class CursorWrapper(object):
    def __init__(self, cursor: Any):
        self.cursor = cursor

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetch_one()
        if row is None:
            raise StopIteration()
        return row

    def fetch_all(self):
        return [DictWrapper(row.asDict()) for row in self.cursor.fetchall()]

    def fetch_many(self, size: int):
        return [DictWrapper(row.asDict()) for row in self.cursor.fetchmany(size)]

    def fetch_one(self):
        row = self.cursor.fetchone().asDict() if self.cursor.fetchone() else None
        if row is not None:
            return DictWrapper(row)
        else:
            self.cursor.close()
        return row


class Database(object):
    def __enter__(self):
        return self

    def __exit__(
        self, exception_type: Any, exception_value: Any, exception_traceback: Any
    ):
        if (
            exception_type is None
            and exception_value is None
            and exception_traceback is None
        ):
            self.connection.commit()
        else:
            self.connection.rollback()
        self.connection.cursor().close()
        self.connection.close()

    def __init__(self, configuration: Configuration = None):
        self.configuration = (
            Configuration.instance() if configuration is None else configuration
        )
        self.command_directory = self.configuration.command_directory
        self.connection = sql.connect(
            access_token=self.configuration.access_token,
            http_path=self.configuration.http_path,
            server_hostname=self.configuration.server_hostname,
        )

    def delete(self, table: str):
        return DeleteBuilder(self, table)

    def execute(self, command: str, parameters: dict = None, skip_load: bool = True):
        cursor = self.connection.cursor()
        if skip_load:
            command = command
        else:
            command = self.load(command, parameters)
        cursor.execute(command, parameters)
        return CursorWrapper(cursor)

    def insert(self, table: str):
        return InsertBuilder(self, table)

    def load(self, command: str, parameters: dict = None):
        command = command.replace(".sql", "")
        try:
            with open(self.command_directory + command + ".sql") as file:
                command = file.read()
            if not parameters:
                return command
            else:
                command = pystache.render(command, parameters)
                return command
        except IOError as exception:
            if exception.errno == errno.ENOENT:
                return command
            else:
                raise exception

    def paging(
        self,
        command: str,
        page: int = 0,
        parameters: dict = None,
        size: int = 10,
        skip_load: bool = True,
    ):
        if skip_load:
            command = command
        else:
            command = self.load(command, parameters)
        command = "{} LIMIT {} OFFSET {}".format(command, size + 1, page * size)
        data = self.execute(command, parameters=parameters, skip_load=True).fetch_all()
        last = len(data) <= size
        return Page(page, size, data[:-1] if not last else data, last)

    def select(self, table: str):
        return SelectBuilder(self, table)

    def update(self, table: str):
        return UpdateBuilder(self, table)


class DictWrapper(dict):
    def __getattr__(self, item: Any):
        if item in self:
            if isinstance(self[item], dict) and not isinstance(self[item], DictWrapper):
                self[item] = DictWrapper(self[item])
            return self[item]
        raise AttributeError()

    def __init__(self, data: Any):
        super().__init__()
        self.update(data)

    def __setattr__(self, key: str, value: Any):
        self[key] = value


class CommandBuilder(object):
    def __init__(self, database: Any, table: str):
        self.database = database
        self.parameters = {}
        self.table = table
        self.where_conditions = []

    def command(self):
        pass

    def execute(self):
        return self.database.execute(
            self.command(), parameters=self.parameters, skip_load=True
        )

    def where(
        self, field: str, value: Any, constant: bool = False, operator: str = "="
    ):
        if constant:
            self.where_conditions.append("{} {} {}".format(field, operator, value))
        else:
            self.parameters[field] = value
            self.where_conditions.append("{0} {1} %({0})s".format(field, operator))
        return self

    def where_all(self, data: Any):
        for value in data.keys():
            self.where(value, data[value])
        return self

    def where_build(self):
        if len(self.where_conditions) > 0:
            conditions = " AND ".join(self.where_conditions)
            return "WHERE {}".format(conditions)
        else:
            return ""


class DeleteBuilder(CommandBuilder):
    def command(self):
        return "DELETE FROM {} {}".format(self.table, self.where_build())


class InsertBuilder(CommandBuilder):
    def __init__(self, database: Database, table: str):
        super(InsertBuilder, self).__init__(database, table)
        self.constants = {}

    def command(self):
        if len(set(list(self.parameters.keys()) + list(self.constants.keys()))) == len(
            self.parameters.keys()
        ) + len(self.constants.keys()):
            columns = []
            values = []
            for field in self.constants:
                columns.append(field)
                values.append(self.constants[field])
            for field in self.parameters:
                columns.append(field)
                values.append("%({})s".format(field))
            return "INSERT INTO {} ({}) VALUES ({})".format(
                self.table, ", ".join(columns), ", ".join(values)
            )
        else:
            raise ValueError()

    def set(self, field: str, value: Any, constant: bool = False):
        if constant:
            self.constants[field] = value
        else:
            self.parameters[field] = value
        return self

    def set_all(self, data: Any):
        for value in data.keys():
            self.set(value, data[value])
        return self


class Page(dict):
    def __init__(self, page_number: int, page_size: int, data, last):
        super().__init__()
        self["data"] = self.data = data
        self["last"] = self.last = last
        self["page_number"] = self.page_number = page_number
        self["page_size"] = self.page_size = page_size


class SelectBuilder(CommandBuilder):
    def __init__(self, database: Database, table: str):
        super(SelectBuilder, self).__init__(database, table)
        self.select_fields = ["*"]
        self.select_group_by = []
        self.select_order_by = []
        self.select_page = ""

    def command(self):
        group_by = ", ".join(self.select_group_by)
        if group_by != "":
            group_by = "GROUP BY {}".format(group_by)
        order_by = ", ".join(self.select_order_by)
        if order_by != "":
            order_by = "ORDER BY {}".format(order_by)
        return "SELECT {} FROM {} {} {} {} {}".format(
            ", ".join(self.select_fields),
            self.table,
            self.where_build(),
            group_by,
            order_by,
            self.select_page,
        )

    def fields(self, *fields: Any):
        self.select_fields = fields
        return self

    def group_by(self, *fields: Any):
        self.select_group_by = fields
        return self

    def order_by(self, *fields: Any):
        self.select_order_by = fields
        return self

    def paging(self, page: int = 0, size: int = 10):
        self.select_page = "LIMIT {} OFFSET {}".format(size + 1, page * size)
        data = self.execute().fetch_all()
        last = len(data) <= size
        return Page(page, size, data[:-1] if not last else data, last)


class UpdateBuilder(CommandBuilder):
    def __init__(self, database: Database, table: str):
        super(UpdateBuilder, self).__init__(database, table)
        self.statements = []

    def command(self):
        return f"UPDATE {self.table} {self.set_build()} {self.where_build()}"

    def set(self, field: str, value: Any, constant: bool = False):
        if constant:
            self.statements.append("{} = {}".format(field, value))
        else:
            self.statements.append("{0} = %({0})s".format(field))
            self.parameters[field] = value
        return self

    def set_all(self, data: Any):
        for value in data.keys():
            self.set(value, data[value])
        return self

    def set_build(self):
        if len(self.statements) > 0:
            statements = ", ".join(self.statements)
            return f"SET {statements}"
        else:
            return ""
