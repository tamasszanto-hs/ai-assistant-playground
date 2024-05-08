import boto3
from botocore.exceptions import ClientError
import logging

# Setting up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StorageException(Exception):
    """Custom exception class for storage errors."""

    def __init__(self, message):
        super().__init__(message)


class StorageManager:
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name, table_name):
        """Initialize the DynamoDB client and set the table name."""
        self.dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        self.table = self.dynamodb.Table(table_name)

    def store_items(self, items):
        """Efficiently store items in DynamoDB table if they are not already present.

        Args:
            items (list of dict): List containing key-value pairs to store.

        Raises:
            StorageException: If any error occurs during data storage.
        """
        try:
            # Retrieve all existing IDs first to minimize writes
            existing_ids = self._get_existing_ids([item['id'] for item in items])
            new_items = [item for item in items if item['id'] not in existing_ids]

            # Using batch_writer to handle batch operations
            with self.table.batch_writer() as batch:
                for item in new_items:
                    batch.put_item(Item=item)
                    logger.info(f"Inserted item with id {item['id']}")
        except ClientError as e:
            logger.error(f"Failed to insert items: {e}")
            raise StorageException(f"Failed to store items: {str(e)}")

    def _get_existing_ids(self, ids):
        """Helper function to fetch existing item IDs in batch."""
        existing_ids = set()
        for id in ids:
            response = self.table.get_item(Key={'id': id})
            if 'Item' in response:
                existing_ids.add(id)
                logger.debug(f"Item with id {id} already exists.")
        return existing_ids


# Example usage of the StorageManager class
if __name__ == "__main__":
    manager = StorageManager("your_access_key_id", "your_secret_access_key", "your_region", "your_table_name")
    try:
        manager.store_items([{'id': '001', 'value': 'example1'}, {'id': '002', 'value': 'example2'}])
    except StorageException as e:
        logger.error(f"Storage operation failed: {str(e)}")
