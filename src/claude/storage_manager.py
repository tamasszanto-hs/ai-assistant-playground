import boto3
import logging
from botocore.exceptions import ClientError

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class StorageException(Exception):
    """Custom exception for storage-related errors."""
    pass


class StorageManager:
    def __init__(self, aws_access_key_id, aws_secret_access_key, table_name):
        self.table_name = table_name
        self.dynamodb = boto3.resource('dynamodb', aws_access_key_id=aws_access_key_id,
                                       aws_secret_access_key=aws_secret_access_key)
        self.table = self.dynamodb.Table(table_name)

    def store_key_value_pairs(self, key_value_pairs):
        """
        Stores a list of key-value pairs in the DynamoDB table.

        Args:
            key_value_pairs (list): A list of dictionaries, where each dictionary has 'key' and 'value' keys.

        Raises:
            StorageException: If there is an error storing the key-value pairs.
        """
        try:
            existing_items = self._get_existing_items([pair['key'] for pair in key_value_pairs])
            items_to_put = [pair for pair in key_value_pairs if pair['key'] not in existing_items]

            if items_to_put:
                logging.info(f"Storing {len(items_to_put)} key-value pairs in the table.")
                with self.table.batch_writer() as batch:
                    for item in items_to_put:
                        batch.put_item(Item=item)
            else:
                logging.info("No new key-value pairs to store.")
        except ClientError as e:
            logging.error(f"Error storing key-value pairs: {e}")
            raise StorageException(f"Error storing key-value pairs: {e}")

    def _get_existing_items(self, keys):
        """
        Retrieves existing items from the DynamoDB table for the given keys.

        Args:
            keys (list): A list of keys to retrieve from the table.

        Returns:
            set: A set of keys that exist in the table.
        """
        existing_items = set()
        try:
            for key in keys:
                response = self.table.get_item(Key={'key': key})
                if 'Item' in response:
                    existing_items.add(key)
        except ClientError as e:
            logging.error(f"Error retrieving existing items: {e}")
        return existing_items
