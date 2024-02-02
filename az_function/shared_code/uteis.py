import os
from azure.storage.blob import BlobServiceClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.identity import ClientSecretCredential 

connection_string = os.environ['azf_blob_endpoint']
c_id = os.environ["adf_client_id"]
c_secret = os.environ["adf_client_secret"]
t_id = os.environ["tenant_id"]
s_id = os.environ["adf_subscription_id"]
rg_name = "RG01"
df_name = "rc-adf-01"
p_name = "PL_databricks_flow"

def uploadToBlobStorage(data,file_name):
   blob_service_client = BlobServiceClient.from_connection_string(connection_string)
   blob_client = blob_service_client.get_blob_client(container='dax', blob=file_name)
   blob_client.upload_blob(data)

def call_pipeline(c_id,c_secret,t_id,s_id,rg_name,df_name,p_name):
    global adf_client
    credentials = ClientSecretCredential(client_id = c_id, client_secret = c_secret, tenant_id=t_id)
    adf_client = DataFactoryManagementClient(credentials, s_id)
    adf_client.pipelines.create_run(rg_name, df_name, p_name)