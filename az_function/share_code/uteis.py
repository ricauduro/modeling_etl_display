import os
from azure.storage.blob import BlobServiceClient

connection_string = os.environ['azf_blob_endpoint']

def uploadToBlobStorage(data,file_name):
   blob_service_client = BlobServiceClient.from_connection_string(connection_string)
   blob_client = blob_service_client.get_blob_client(container='dax', blob=file_name)
   blob_client.upload_blob(data)