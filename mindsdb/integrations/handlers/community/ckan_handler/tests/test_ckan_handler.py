import unittest
from unittest.mock import patch, MagicMock
from mindsdb.integrations.handlers.ckan_handler.ckan_handler import CkanHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb_sql_parser import ast


class CkanHandlerTest(unittest.TestCase):
    def setUp(self):
        self.handler = CkanHandler('test_ckan_handler', connection_data={
            "url": "http://mock-ckan-url.com",
            "api_key": "mock_api_key"
        })
        self.patcher = patch('mindsdb.integrations.handlers.ckan_handler.ckan_handler.RemoteCKAN')
        self.mock_ckan = self.patcher.start()
        self.mock_ckan_instance = MagicMock()
        self.mock_ckan.return_value = self.mock_ckan_instance

    def tearDown(self):
        self.patcher.stop()

    def test_check_connection(self):
        self.mock_ckan_instance.action.site_read.return_value = True
        response = self.handler.check_connection()
        self.assertTrue(response.success)

    def test_get_tables(self):
        self.mock_ckan_instance.action.package_list.return_value = [
            "package1",
            "package2",
        ]
        result = self.handler.get_tables()
        self.assertIsNotNone(result)
        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)

    def test_query_package_ids(self):
        mock_packages = [
            {
                "id": "pkg1",
                "name": "Package 1",
                "title": "Test Package 1",
                "num_resources": 2,
            },
            {
                "id": "pkg2",
                "name": "Package 2",
                "title": "Test Package 2",
                "num_resources": 1,
            },
        ]
        self.mock_ckan_instance.action.package_search.return_value = {
            "results": mock_packages
        }

        query = ast.Select(
            targets=[ast.Star()],
            from_table=ast.Identifier("package_ids"),
            limit=ast.Constant(10),
        )
        result = self.handler.query(query)
        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(len(result.data_frame), 2)

    def test_query_resource_ids(self):
        mock_packages = [
            {
                "id": "pkg1",
                "resources": [
                    {
                        "id": "res1",
                        "name": "Resource 1",
                        "format": "CSV",
                        "datastore_active": True,
                    },
                    {
                        "id": "res2",
                        "name": "Resource 2",
                        "format": "XSLX",
                        "datastore_active": False,
                    },
                ],
            },
            {
                "id": "pkg2",
                "resources": [
                    {
                        "id": "res3",
                        "name": "Resource 3",
                        "format": "CSV",
                        "datastore_active": True,
                    }
                ],
            },
        ]
        self.mock_ckan_instance.action.package_search.return_value = {
            "results": mock_packages
        }

        query = ast.Select(
            targets=[ast.Star()],
            from_table=ast.Identifier("resource_ids"),
            limit=ast.Constant(10),
        )
        result = self.handler.query(query)
        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(len(result.data_frame), 2)

    def test_query_datastore_without_resource_id(self):
        query = ast.Select(
            targets=[ast.Star()],
            from_table=ast.Identifier("datastore"),
            limit=ast.Constant(10),
        )
        result = self.handler.query(query)
        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(len(result.data_frame), 1)
        self.assertIn("message", result.data_frame.columns)

    def test_query_datastore_with_resource_id(self):
        mock_records = [
            {"id": 1, "name": "Record 1"},
            {"id": 2, "name": "Record 2"},
            {"id": 3, "name": "Record 3"},
        ]
        self.mock_ckan_instance.action.datastore_search.return_value = {
            "records": mock_records
        }

        query = ast.Select(
            targets=[ast.Star()],
            from_table=ast.Identifier("datastore"),
            where=ast.BinaryOperation(
                "=",
                args=[ast.Identifier("resource_id"), ast.Constant("test_resource_id")],
            ),
            limit=ast.Constant(10),
        )
        result = self.handler.query(query)
        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(len(result.data_frame), 3)

    def test_native_query(self):
        self.mock_ckan_instance.action.package_list.return_value = [
            "package1",
            "package2",
            "package3",
        ]

        query = "package_list:"
        result = self.handler.native_query(query)
        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(len(result.data_frame), 3)


if __name__ == "__main__":
    unittest.main()
