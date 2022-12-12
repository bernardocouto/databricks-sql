from databricks_sql.main import Configuration, Database

import os
import unittest

CONFIGURATION = Configuration.instance(
    access_token="",
    command_directory=os.path.realpath(os.path.curdir) + "/commands/",
    http_path="",
    server_hostname="",
)


class TestDefault(unittest.TestCase):

    database = None

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self.database = Database(CONFIGURATION)

    def test_execute_command_fetch_all(self):
        with self.database as connection:
            result = connection.execute(
                "SELECT * FROM catalog.schema.table LIMIT 100", skip_load=True
            ).fetch_all()

    def test_execute_command_fetch_many(self):
        with self.database as connection:
            result = connection.execute(
                "SELECT * FROM catalog.schema.table LIMIT 10", skip_load=True
            ).fetch_many(10)

    def test_execute_command_fetch_one(self):
        with self.database as connection:
            result = connection.execute(
                "SELECT * FROM catalog.schema.table LIMIT 1", skip_load=True
            ).fetch_one()

    def test_execute_file_fetch_all(self):
        with self.database as connection:
            result = connection.execute(
                "test_execute_file_fetch_all.sql", skip_load=False
            ).fetch_all()

    def test_execute_file_fetch_many(self):
        with self.database as connection:
            result = connection.execute(
                "test_execute_file_fetch_many.sql", skip_load=False
            ).fetch_many(10)

    def test_execute_file_fetch_one(self):
        with self.database as connection:
            result = connection.execute(
                "test_execute_file_fetch_one.sql", skip_load=False
            ).fetch_one()

    def test_execute_file_fetch_all_with_parameter(self):
        with self.database as connection:
            result = connection.execute(
                "test_execute_file_fetch_all_with_parameter.sql",
                parameters={"limit": 100},
                skip_load=False,
            ).fetch_all()

    def test_execute_file_fetch_many_with_parameter(self):
        with self.database as connection:
            result = connection.execute(
                "test_execute_file_fetch_many_with_parameter.sql",
                parameters={"limit": 10},
                skip_load=False,
            ).fetch_many(10)

    def test_execute_file_fetch_one_with_parameter(self):
        with self.database as connection:
            result = connection.execute(
                "test_execute_file_fetch_one_with_parameter.sql",
                parameters={"limit": 1},
                skip_load=False,
            ).fetch_one()
