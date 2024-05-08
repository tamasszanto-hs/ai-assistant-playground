import unittest
from unittest.mock import MagicMock, patch
from storage_manager import StorageManager, StorageException
from botocore.exceptions import ClientError


class TestStorageManager(unittest.TestCase):
    def setUp(self):
        self.table_name = 'test_table'
        self.storage_manager = StorageManager('test_key', 'test_secret', self.table_name, max_workers=2)

    @patch('storage_manager.StorageManager._get_existing_items')
    def test_store_key_value_pairs_new_items(self, mock_get_existing_items):
        mock_get_existing_items.return_value = set()
        key_value_pairs = [{'key': 'key1', 'value': 'value1'}, {'key': 'key2', 'value': 'value2'}]

        with patch.object(self.storage_manager.dynamodb, 'batch_write_item') as mock_batch_write_item:
            self.storage_manager.store_key_value_pairs(key_value_pairs, batch_size=1)

            expected_requests = [
                {self.table_name: [{'PutRequest': {'Item': {'key': 'key1', 'value': 'value1'}}}]},
                {self.table_name: [{'PutRequest': {'Item': {'key': 'key2', 'value': 'value2'}}}]}
            ]
            mock_batch_write_item.assert_has_calls(
                [unittest.mock.call(RequestItems=request) for request in expected_requests], any_order=True)

    @patch('storage_manager.StorageManager._get_existing_items')
    def test_store_key_value_pairs_existing_items(self, mock_get_existing_items):
        mock_get_existing_items.return_value = {'key1'}
        key_value_pairs = [{'key': 'key1', 'value': 'value1'}, {'key': 'key2', 'value': 'value2'}]

        with patch.object(self.storage_manager.dynamodb, 'batch_write_item') as mock_batch_write_item:
            self.storage_manager.store_key_value_pairs(key_value_pairs)

            expected_request = {self.table_name: [{'PutRequest': {'Item': {'key': 'key2', 'value': 'value2'}}}]}
            mock_batch_write_item.assert_called_once_with(RequestItems=expected_request)

    @patch('storage_manager.StorageManager._get_existing_items')
    def test_store_key_value_pairs_error(self, mock_get_existing_items):
        mock_get_existing_items.side_effect = ClientError({'Error': {'Code': 'TestException'}}, 'get_item')
        key_value_pairs = [{'key': 'key1', 'value': 'value1'}]

        with self.assertRaises(StorageException):
            self.storage_manager.store_key_value_pairs(key_value_pairs)


if __name__ == '__main__':
    unittest.main()
