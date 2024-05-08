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
        """Store items in DynamoDB table if they are not already present.

        Args:
            items (list of dict): List containing key-value pairs to store.

        Raises:
            StorageException: If any error occurs during data storage.
        """
        for item in items:
            try:
                # Check if the item already exists
                response = self.table.get_item(Key={'id': item['id']})
                if 'Item' not in response:
                    # Item does not exist, proceed to insert
                    self.table.put_item(Item=item)
                    logger.info(f"Inserted item with id {item['id']}")
                else:
                    logger.debug(f"Item with id {item['id']} already exists.")
            except ClientError as e:
                logger.error(f"Failed to insert item with id {item['id']}: {e}")
                raise StorageException(f"Failed to store item with id {item['id']}: {str(e)}")


# Example usage of the StorageManager class
if __name__ == "__main__":
    manager = StorageManager("your_access_key_id", "your_secret_access_key", "your_region", "your_table_name")
    try:
        manager.store_items([{'id': '001', 'value': 'example1'}, {'id': '002', 'value': 'example2'}])
    except StorageException as e:
        logger.error(f"Storage operation failed: {str(e)}")
