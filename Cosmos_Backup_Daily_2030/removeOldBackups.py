import logging
from datetime import datetime, timedelta
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# Set environment variabls
blob_endpoint = os.environ.get('BLOB_ENDPOINT')
blob_endpoint = os.environ.get('BLOB_BACKUP_CONTAINER')

# Define Storage connection details
credential = DefaultAzureCredential()
blob_service_client = BlobServiceClient(account_url=blob_endpoint, credential=credential)
container_name = blob_endpoint

# Remove files older than x days
def remove_old_files_from_container():
    try:
        container_client = blob_service_client.get_container_client(container_name)
        for blob in container_client.list_blobs():
            if blob.creation_time < (datetime.utcnow() - timedelta(days=30)):
                container_client.delete_blob(blob)
        logging.info("Old files removed from container.")
    except Exception as e:
        logging.error(f"Error removing old files from container: {e}")
