import unittest
from unittest.mock import patch, MagicMock

from storage_manager import StorageManager


class TestStorageManager(unittest.TestCase):
    @patch('boto3.resource')
    def test_store_items(self, mock_dynamodb_resource):
        # Set up the DynamoDB table mock
        mock_table = MagicMock()
        mock_table.get_item.side_effect = [
            {'Item': {'id': '001', 'value': 'example1'}},  # Item exists
            {}  # Item does not exist
        ]
        mock_dynamodb_resource.return_value.Table.return_value = mock_table

        # Initialize StorageManager with mock parameters
        manager = StorageManager("access_key", "secret_key", "region", "table_name")

        # Define the test data
        test_items = [{'id': '001', 'value': 'example1'}, {'id': '003', 'value': 'example3'}]

        # Perform the test
        manager.store_items(test_items)

        # Check calls to mock
        mock_table.get_item.assert_called_with(Key={'id': '003'})
        mock_table.put_item.assert_called_once_with(Item={'id': '003', 'value': 'example3'})


if __name__ == '__main__':
    unittest.main()
