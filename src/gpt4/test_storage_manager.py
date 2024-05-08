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

        # Mock the batch writer
        mock_batch_writer = MagicMock()
        mock_table.batch_writer.return_value.__enter__.return_value = mock_batch_writer

        # Initialize StorageManager with mock parameters
        manager = StorageManager("access_key", "secret_key", "region", "table_name")

        # Define the test data
        test_items = [{'id': '001', 'value': 'example1'}, {'id': '003', 'value': 'example3'}]

        # Perform the test
        manager.store_items(test_items)

        # Check calls to get_item to ensure only necessary checks are made
        mock_table.get_item.assert_any_call(Key={'id': '001'})
        mock_table.get_item.assert_any_call(Key={'id': '003'})

        # Verifying that put_item was called correctly inside batch_writer context
        mock_batch_writer.put_item.assert_called_once_with(Item={'id': '003', 'value': 'example3'})

        # Verify the total number of get_item calls (should match the number of ids being checked)
        self.assertEqual(mock_table.get_item.call_count, 2)


if __name__ == '__main__':
    unittest.main()
