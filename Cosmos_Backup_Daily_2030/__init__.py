import logging
import azure.functions as func
import os
from . import backupCollections as bc
from . import removeOldBackups as rob

# Set environment variable from function app
collections_string = os.environ.get('COLLECTIONS')

# Convert collections_string into an array
if collections_string:
    collections_array = collections_string.split(',')
else:
    logging.error("COLLECTIONS environment variable is not set.")

# Run backupCollections.py with the collections_array
def main(mytimer: func.TimerRequest) -> None:
    logging.info('Python timer trigger function processed a request.')
    for collection_name in collections_array:
        bc.connect_to_mongodb_and_write_to_azure_storage(collection_name)

    # Remove old files after processing all collections
    rob.remove_old_files_from_container()
