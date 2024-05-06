import json
import logging
import os
from bson import ObjectId
from pymongo import MongoClient
from datetime import datetime, timedelta
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# Set environment variabls
database_connection_string = os.environ.get('DATABASE_CONNECTION_STRING')
database = os.environ.get('DATABASE')
blob_endpoint = os.environ.get('BLOB_ENDPOINT')
blob_backup_container = os.environ.get('BLOB_BACKUP_CONTAINER')

# Define MongoDB and Storage connection details
mongo_uri = database_connection_string
mongo_database = database
credential = DefaultAzureCredential()
blob_service_client = BlobServiceClient(account_url=blob_endpoint, credential=credential)
container_name = blob_backup_container

# This function is used to serialize the mongo db document id 
def custom_encoder(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f"{type(obj)} is not JSON serializable")

# Connect to mongo db and backup collection to storage
def connect_to_mongodb_and_write_to_azure_storage(collection_name):
    try:
        client = MongoClient(mongo_uri)
        database = client[mongo_database]
        collection = database[collection_name]

        documents = list(collection.find())
        if documents:
            aest_date = (datetime.utcnow() + timedelta(hours=10)).strftime("%Y-%m-%d")
            json_documents = [json.dumps(doc, default=custom_encoder) for doc in documents]
            combined_json_string = "[" + ",".join(json_documents) + "]"

            blob_name = f"{collection_name}_{aest_date}.json"
            container_client = blob_service_client.get_container_client(container_name)
            blob_client = container_client.get_blob_client(blob=blob_name)
            blob_client.upload_blob(combined_json_string)

            logging.info(f"All documents from {collection_name} uploaded to Azure Storage: {blob_name}")
        else:
            logging.error("No document found in the collection.")

        client.close()
    except Exception as e:
        logging.error(f"Error: {e}")
