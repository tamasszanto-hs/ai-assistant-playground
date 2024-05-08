import boto3
import logging
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class StorageException(Exception):
    """Custom exception for storage-related errors."""
    pass


class StorageManager:
    def __init__(self, aws_access_key_id, aws_secret_access_key, table_name, max_workers=10):
        self.table_name = table_name
        self.dynamodb = boto3.resource('dynamodb', aws_access_key_id=aws_access_key_id,
                                       aws_secret_access_key=aws_secret_access_key)
        self.table = self.dynamodb.Table(table_name)
        self.max_workers = max_workers

    def store_key_value_pairs(self, key_value_pairs, batch_size=25):
        """
        Stores a list of key-value pairs in the DynamoDB table using concurrent requests.

        Args:
            key_value_pairs (list): A list of dictionaries, where each dictionary has 'key' and 'value' keys.
            batch_size (int): The maximum number of items to include in a single batch request.

        Raises:
            StorageException: If there is an error storing the key-value pairs.
        """
        try:
            existing_items = self._get_existing_items([pair['key'] for pair in key_value_pairs])
            items_to_put = [pair for pair in key_value_pairs if pair['key'] not in existing_items]

            if items_to_put:
                logging.info(f"Storing {len(items_to_put)} key-value pairs in the table.")
                batches = [items_to_put[i:i + batch_size] for i in range(0, len(items_to_put), batch_size)]
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [executor.submit(self._batch_write, batch) for batch in batches]
                    for future in futures:
                        future.result()  # Raise any exceptions from the futures
            else:
                logging.info("No new key-value pairs to store.")
        except ClientError as e:
            logging.error(f"Error storing key-value pairs: {e}")
            raise StorageException(f"Error storing key-value pairs: {e}")

    def _batch_write(self, batch):
        """
        Performs a batch write operation for the given batch of items.

        Args:
            batch (list): A list of dictionaries, where each dictionary has 'key' and 'value' keys.
        """
        request_items = {
            self.table_name: [
                {'PutRequest': {'Item': item}} for item in batch
            ]
        }
        self.dynamodb.batch_write_item(RequestItems=request_items)

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
