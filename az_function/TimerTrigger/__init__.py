import os
import json
import logging
import requests
from datetime import datetime
import azure.functions as func
from novadax import RequestClient as NovaClient
from azure.storage.blob import BlobServiceClient
from shared_code.uteis import uploadToBlobStorage

AccessKey = os.environ['dax_access_key']
SecretKey = os.environ['dax_secret']
connection_string = os.environ['azf_blob_endpoint']

def main(mytimer: func.TimerRequest) -> None:
    logging.info('The timer is past due!')
    if mytimer.past_due:
        logging.info('The timer is past due!')

        nova_client = NovaClient(AccessKey, SecretKey)
        result = nova_client.get_ticker('BTC_BRL')
        filename_date = datetime.now().strftime('%Y%m%d_%H%M%S')
        res = json.dumps(result)
        logging.info(res)
        uploadToBlobStorage(res, 'dax_{}'.format(filename_date))

    logging.info('Python timer trigger function executed.')