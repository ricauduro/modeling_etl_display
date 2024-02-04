import os
import json
import logging
from datetime import datetime
import azure.functions as func
from novadax import RequestClient as NovaClient
from shared_code.uteis import uploadToBlobStorage, call_pipeline

#blob variable
access_key = os.environ['dax_access_key']
secret_key = os.environ['dax_secret']

#adf variables
rg_name = "RG01"
df_name = "rc-adf-01"
p_name = "PL_databricks_flow"

c_id = os.environ["adf_client_id"]
c_secret = os.environ["adf_client_secret"]
t_id = os.environ["tenant_id"]
s_id = os.environ["adf_subscription_id"]

def main(mytimer: func.TimerRequest) -> None:
    nova_client = NovaClient(access_key, secret_key)
    result = nova_client.get_ticker('BTC_BRL')
    filename_date = datetime.now().strftime('%Y%m%d_%H%M%S')
    res = json.dumps(result)
    uploadToBlobStorage(res, 'dax_{}.json'.format(filename_date))

    logging.info('Python timer trigger function executed.')
    call_pipeline(c_id, c_secret, t_id, s_id, rg_name, df_name, p_name)