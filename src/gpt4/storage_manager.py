import boto3
from botocore.exceptions import ClientError
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setting up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StorageException(Exception):
    """Custom exception class for storage errors."""

    def __init__(self, message):
        super().__init__(message)


class StorageManager:
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name, table_name):
        self.dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        self.table = self.dynamodb.Table(table_name)

    def store_items(self, items):
        """Uses ThreadPoolExecutor to efficiently check and write items."""
        # Retrieve all existing IDs to minimize writes
        existing_ids = self._get_existing_ids([item['id'] for item in items])

        # Filter items not in existing_ids
        new_items = [item for item in items if item['id'] not in existing_ids]

        # Use ThreadPoolExecutor to perform batch writes
        self._batch_write(new_items)

    def _get_existing_ids(self, ids):
        """Retrieve existing item IDs using concurrent threads."""
        existing_ids = set()
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(self._check_id_existence, id): id for id in ids}
            for future in as_completed(futures):
                id, exists = future.result()
                if exists:
                    existing_ids.add(id)
        return existing_ids

    def _check_id_existence(self, id):
        """Check if a single id exists in the table."""
        try:
            response = self.table.get_item(Key={'id': id})
            return (id, 'Item' in response)
        except ClientError as e:
            logger.error(f"Failed to check item with id {id}: {e}")
            raise StorageException(f"Failed to check item existence with id {id}: {str(e)}")

    def _batch_write(self, items):
        """Write items in batches using ThreadPoolExecutor."""
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(self._write_item, item) for item in items]
            for future in as_completed(futures):
                try:
                    future.result()  # This will re-raise any exceptions caught during the _write_item calls
                except StorageException as e:
                    logger.error(f"Batch write failed: {str(e)}")

    def _write_item(self, item):
        """Write a single item to the DynamoDB table."""
        try:
            self.table.put_item(Item=item)
            logger.info(f"Inserted item with id {item['id']}")
        except ClientError as e:
            logger.error(f"Failed to insert item with id {item['id']}: {e}")
            raise StorageException(f"Failed to insert item with id {item['id']}: {str(e)}")


# Example usage of the StorageManager class
if __name__ == "__main__":
    manager = StorageManager("your_access_key_id", "your_secret_access_key", "your_region", "your_table_name")
    try:
        manager.store_items([{'id': '001', 'value': 'example1'}, {'id': '002', 'value': 'example2'}])
    except StorageException as e:
        logger.error(f"Storage operation failed: {str(e)}")
