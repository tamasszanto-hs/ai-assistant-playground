import unittest
from unittest.mock import patch, MagicMock, call
from concurrent.futures import Future

from storage_manager import StorageManager


def make_future(return_value):
    """Utility function to create a future instance with a set result."""
    future = Future()
    future.set_result(return_value)
    return future


class TestStorageManager(unittest.TestCase):
    @patch('boto3.resource')
    @patch('concurrent.futures.ThreadPoolExecutor')
    def test_store_items(self, mock_executor, mock_dynamodb_resource):
        # Set up the DynamoDB table mock
        mock_table = MagicMock()
        mock_dynamodb_resource.return_value.Table.return_value = mock_table

        # Mock the responses for get_item to simulate existence checks
        mock_table.get_item.side_effect = [
            {'Item': {'id': '001', 'value': 'example1'}},  # Item 001 exists
            {}  # Item 003 does not exist
        ]

        # Set up the ThreadPoolExecutor mock
        mock_executor.return_value.__enter__.return_value = mock_executor

        # Mock the future responses for each submit call
        mock_executor.submit.side_effect = [
            make_future(('001', True)),  # Check existence for item 001
            make_future(('003', False)),  # Check existence for item 003
            make_future(None)  # Writing item 003 (we don't care about the return in this case)
        ]

        # Initialize StorageManager with mock parameters
        manager = StorageManager("access_key", "secret_key", "region", "table_name")

        # Define the test data
        test_items = [{'id': '001', 'value': 'example1'}, {'id': '003', 'value': 'example3'}]

        # Perform the test
        manager.store_items(test_items)

        # Verify the expected calls to get_item
        calls = [call(Key={'id': '001'}), call(Key={'id': '003'})]
        mock_table.get_item.assert_has_calls(calls, any_order=True)

        # Verify put_item was called correctly for the new item
        mock_table.put_item.assert_called_once_with(Item={'id': '003', 'value': 'example3'})


if __name__ == '__main__':
    unittest.main()
