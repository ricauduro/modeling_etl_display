## DB Modeling, ETL and display -> end-to-end project 
  This file will hold the project drive-thru, starting from modeling a DB, ingesting data with an Azure function from an API, creating a dimensional model with existing DB tables + API data, then displaying the final results with a PowerBI dashbord.
  I´ll use data from a bitcoin broker and myself data. So I´m planning to create the following entities:
  
```md
    -user
    -broker
    -address
    -phone

  **Users and brokers must have phones and addresses. Users can have only one broker. 
```
  An AzureSQL will hold the entities while an Azure Function will consume data from an API on an hourly basis in a blob storage.

  Once the entities and API data are in place, a Databricks notebook will transform API data and then move it to AzureSQL DB.
  
  With all tables created, then we´ll be ready to create our dimensional model with a star schema and then display the results with PowerBI

### Data modeling

  As I´m starting from scratch, let´s build the ERD (Entity-Relationship Diagram). I´m using brModelo to do it. You can check it out here https://github.com/chcandido/brModelo

Knowing that 
1)users and brokers must have phones and addresses and 
2)Users can have only one broker, we can build the model with this cardinality:

![image](https://github.com/ricauduro/modeling_etl_display/assets/58055908/4c5c1b63-233a-4d6d-b4ce-0fd214d73496)

Our conceptual model looks like this

![image](https://github.com/ricauduro/modeling_etl_display/assets/58055908/dbf5cf17-c45d-43cc-b706-dd8721e5583b)



Which lead to our logical model

![image](https://github.com/ricauduro/modeling_etl_display/assets/58055908/45dd9534-4072-4980-bbe2-18130f1710f0)


and then to our fisical schema

```sql

CREATE TABLE CORRETORA (
CORRETORA_ID INTEGER PRIMARY KEY IDENTITY(1,1),
NOME VARCHAR(10),
ATUACAO VARCHAR(10)
);

CREATE TABLE ENDERECO (
ENDERECO_ID INTEGER PRIMARY KEY IDENTITY(1,1),
CORRETORA_ID INTEGER,
LOGRADOURO VARCHAR(50),
RUA VARCHAR(50),
NUMERO INTEGER,
BAIRRO VARCHAR(20),
CEP VARCHAR(10),
CIDADE VARCHAR(10),
UF VARCHAR(2),
FOREIGN KEY(CORRETORA_ID) REFERENCES CORRETORA (CORRETORA_ID)
);

CREATE TABLE USUARIO (
USUARIO_ID INTEGER PRIMARY KEY IDENTITY(1,1),
ENDERECO_ID INTEGER,
CORRETORA_ID INTEGER,
NOME VARCHAR(20),
NOME_MEIO VARCHAR(20),
SOBRENOME VARCHAR(20),
DATA_NASCIMENTO DATETIME,
FOREIGN KEY(CORRETORA_ID) REFERENCES CORRETORA (CORRETORA_ID),
FOREIGN KEY(ENDERECO_ID) REFERENCES ENDERECO (ENDERECO_ID)
);

CREATE TABLE TELEFONE (
TELEFONE_ID INTEGER PRIMARY KEY IDENTITY(1,1),
USUARIO_ID INTEGER,
CORRETORA_ID INTEGER,
TIPO VARCHAR(10),
DDD INTEGER,
NUMERO INTEGER,
FOREIGN KEY(CORRETORA_ID) REFERENCES CORRETORA (CORRETORA_ID),
FOREIGN KEY(USUARIO_ID) REFERENCES USUARIO (USUARIO_ID)
);

```
Now we have the tables in place, we can insert some data into it

```sql
INSERT INTO CORRETORA (NOME, ATUACAO)
VALUES ('DAX', 'Cripto');

INSERT INTO ENDERECO (CORRETORA_ID, LOGRADOURO, RUA, NUMERO, BAIRRO, CEP, CIDADE, UF)
VALUES (1, 'Avenida',  'da Saudade', 123, 'Bairro A', '12345-678', 'Campinas', 'SP');

INSERT INTO USUARIO (ENDERECO_ID, CORRETORA_ID, NOME, NOME_MEIO, SOBRENOME, DATA_NASCIMENTO)
VALUES (1, 1, 'Ricardo', null, 'Cauduro', '1983-02-25'),
       (1, 1, 'Rita', 'de Cassia', 'Martins Cauduro', '1986-04-30');

INSERT INTO TELEFONE (USUARIO_ID, CORRETORA_ID, TIPO, DDD, NUMERO)
VALUES (1, 1, 'Celular', 19, 987654321),
       (2, 1, 'Celular', 19, 978644822);
```
### Azure Function

Moving on, let´s start to extract our broker´s data using an Azure Function to call a API and save the results into a blob. 

You can check here how to develop and deploy a Azure function https://learn.microsoft.com/en-us/azure/azure-functions/functions-develop-vs-code?tabs=node-v3%2Cpython-v2%2Cisolated-process&pivots=programming-language-python.

You may find this one usefull too https://learn.microsoft.com/EN-us/azure/azure-functions/functions-identity-based-connections-tutorial -> Grant AzFunction KV access

I´m goign to focus on the code, which is very simple as we can see below. We´re using a TimeTrigger function

```python
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

```
This code will run on an hourly basis. Then, with 

```python
        nova_client = NovaClient(AccessKey, SecretKey)
        result = nova_client.get_ticker('BTC_BRL')
```
we´re getting the bitcoin last price and other attributes. After we´re setting an filename, using datetime function 
```python
        filename_date = datetime.now().strftime('%Y%m%d_%H%M%S')
```
and then we´re calling uploadToBlobStorage function, that we imported from shared_code folder. 
```python
        from shared_code.uteis import uploadToBlobStorage

        uploadToBlobStorage(res, 'dax_{}.json'.format(filename_date))
```
This is the function at shared_code folder:

```python
import os
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

connection_string = os.environ['azf_blob_endpoint']

def uploadToBlobStorage(data,file_name):
   blob_service_client = BlobServiceClient.from_connection_string(connection_string)
   blob_client = blob_service_client.get_blob_client(container='dax', blob=file_name)
   blob_client.upload_blob(data)

def call_pipeline(c_id,c_secret,t_id,s_id,rg_name,df_name,p_name):
    global adf_client
    credentials = ClientSecretCredential(client_id = c_id, client_secret = c_secret, tenant_id=t_id)
    adf_client = DataFactoryManagementClient(credentials, s_id)
    adf_client.pipelines.create_run(rg_name, df_name, p_name)
```

After few days running, this is our blob storage

![image](https://github.com/ricauduro/modeling_etl_display/assets/58055908/eed66e57-7266-4f0a-bfe0-fa5113953bd3)

at code´s end we have this funciton, also imported from shared_code, that will create a Data Factory pipeline run, to start Dabricks transformations

```python
        call_pipeline(c_id, c_secret, t_id, s_id, rg_name, df_name, p_name)
```
this is the pipeline

![image](https://github.com/ricauduro/modeling_etl_display/assets/58055908/5f23d161-73a5-4705-8d1a-aae23a0103b5)

it´ll get the max date from SQL table, so when reading API data, which are all togheter inside the container, we can use the date to filter and process only newest data

### Databricks

https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes -> Grant Databricks KV access (this one is in conflict with grant AzFunction KV access... read carefully)
