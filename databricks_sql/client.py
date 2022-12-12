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
        row = self.cursor.fetchone().asDict()
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

    def execute(self, command: str, parameters: dict = None, skip_load: bool = True):
        cursor = self.connection.cursor()
        if skip_load:
            command = command
        else:
            command = self.load(command, parameters)
        cursor.execute(command, parameters)
        return CursorWrapper(cursor)

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
