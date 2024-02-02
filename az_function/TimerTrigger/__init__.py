import os
import json
import logging
import requests
from datetime import datetime
import azure.functions as func
from novadax import RequestClient as NovaClient
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential 
from azure.mgmt.datafactory import DataFactoryManagementClient
from shared_code.uteis import uploadToBlobStorage, call_pipeline

#blob variable
access_key = os.environ['dax_access_key']
secret_key = os.environ['dax_secret']
connection_string = os.environ['azf_blob_endpoint']

#adf variables
rg_name = "RG01"
df_name = "rc-adf-01"
p_name = "PL_databricks_flow"
s_id = os.environ["adf_subscription_id"]
c_id = os.environ["adf_client_id"]
t_id = os.environ["tenant_id"]
c_secret = os.environ["adf_client_secret"]
df_params = {"location":"brazilsouth"}
credentials = ClientSecretCredential(client_id=c_id, client_secret=c_secret, tenant_id=t_id)
adf_client = DataFactoryManagementClient(credentials, s_id)

def main(mytimer: func.TimerRequest) -> None:
    logging.info('The timer is past due!')
    if mytimer.past_due:
        logging.info('The timer is past due!')

        nova_client = NovaClient(access_key, secret_key)
        result = nova_client.get_ticker('BTC_BRL')
        filename_date = datetime.now().strftime('%Y%m%d_%H%M%S')
        res = json.dumps(result)
        logging.info(res)
        uploadToBlobStorage(res, 'dax_{}'.format(filename_date))

    logging.info('Python timer trigger function executed.')
    call_pipeline(c_id,c_secret,t_id,s_id,rg_name,df_name,p_name)