import unittest
from unittest.mock import MagicMock, patch
from storage_manager import StorageManager, StorageException
from botocore.exceptions import ClientError


class TestStorageManager(unittest.TestCase):
    def setUp(self):
        self.table_name = 'test_table'
        self.storage_manager = StorageManager('test_key', 'test_secret', self.table_name)

    @patch('storage_manager.StorageManager._get_existing_items')
    def test_store_key_value_pairs_new_items(self, mock_get_existing_items):
        mock_get_existing_items.return_value = set()
        key_value_pairs = [{'key': 'key1', 'value': 'value1'}, {'key': 'key2', 'value': 'value2'}]

        with patch.object(self.storage_manager.table, 'batch_writer') as mock_batch_writer:
            mock_batch = MagicMock()
            mock_batch_writer.return_value.__enter__.return_value = mock_batch

            self.storage_manager.store_key_value_pairs(key_value_pairs)

            mock_batch.put_item.assert_any_call(Item={'key': 'key1', 'value': 'value1'})
            mock_batch.put_item.assert_any_call(Item={'key': 'key2', 'value': 'value2'})

    @patch('storage_manager.StorageManager._get_existing_items')
    def test_store_key_value_pairs_existing_items(self, mock_get_existing_items):
        mock_get_existing_items.return_value = {'key1'}
        key_value_pairs = [{'key': 'key1', 'value': 'value1'}, {'key': 'key2', 'value': 'value2'}]

        with patch.object(self.storage_manager.table, 'batch_writer') as mock_batch_writer:
            mock_batch = MagicMock()
            mock_batch_writer.return_value.__enter__.return_value = mock_batch

            self.storage_manager.store_key_value_pairs(key_value_pairs)

            mock_batch.put_item.assert_called_once_with(Item={'key': 'key2', 'value': 'value2'})

    @patch('storage_manager.StorageManager._get_existing_items')
    def test_store_key_value_pairs_error(self, mock_get_existing_items):
        mock_get_existing_items.side_effect = ClientError({'Error': {'Code': 'TestException'}}, 'get_item')
        key_value_pairs = [{'key': 'key1', 'value': 'value1'}]

        with self.assertRaises(StorageException):
            self.storage_manager.store_key_value_pairs(key_value_pairs)


if __name__ == '__main__':
    unittest.main()
