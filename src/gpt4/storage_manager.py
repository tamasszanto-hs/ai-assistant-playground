import boto3
from botocore.exceptions import ClientError
import logging
from threading import Thread
from queue import Queue

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
        """Uses threading to efficiently check and write items."""
        # Split work into chunks for threading
        chunk_size = 25  # DynamoDB batch write limit
        chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
        threads = []
        queue = Queue()

        for chunk in chunks:
            thread = Thread(target=self._process_chunk, args=(chunk, queue))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        new_items = []
        while not queue.empty():
            new_items.extend(queue.get())

        # Write new items in batches
        self._batch_write(new_items)

    def _process_chunk(self, chunk, queue):
        """Thread worker for processing a chunk of items."""
        existing_ids = self._get_existing_ids([item['id'] for item in chunk])
        new_items = [item for item in chunk if item['id'] not in existing_ids]
        queue.put(new_items)

    def _get_existing_ids(self, ids):
        existing_ids = set()
        for id in ids:
            response = self.table.get_item(Key={'id': id})
            if 'Item' in response:
                existing_ids.add(id)
        return existing_ids

    def _batch_write(self, items):
        with self.table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)
                logger.info(f"Inserted item with id {item['id']}")


# Example usage of the StorageManager class
if __name__ == "__main__":
    manager = StorageManager("your_access_key_id", "your_secret_access_key", "your_region", "your_table_name")
    try:
        manager.store_items([{'id': '001', 'value': 'example1'}, {'id': '002', 'value': 'example2'}])
    except StorageException as e:
        logger.error(f"Storage operation failed: {str(e)}")
