## DB Modeling, ETL and display -> end-to-end project 
  This file will hold the project drive-thru, starting from modeling a DB, ingesting data with an Azure function from an API, creating a dimensional model with existing DB tables + API data, then displaying the final results with a PowerBI dashbord.

  We´ll use the following services in Azure to build our DB and ETL process
<pre>
  -Azure SQLDB
  -Azure Functions
  -Azure Key Vault
  -Azure Databricks
</pre>

  Outside Azure, we´ll use 
<pre>
  -brModelo to model our DB and our dimensional tables
  -PowerBi to visualize our data 
</pre>
  I´ll use data from a bitcoin broker and myself data. So I´m planning to create the following entities:
  
<pre>
    -user
    -broker
    -address
    -phone

  **Users and brokers must have phones and addresses. Users can have only one broker.
</pre>
  An AzureSQL will hold the entities while an Azure Function will consume data from an API on an hourly basis in a blob storage.

  Once the entities and API data are in place, a Databricks notebook will transform API data and then move it to AzureSQL DB.
  
  With all tables created, then we´ll be ready to create our dimensional model with a star schema and then display the results with PowerBI

### Data modeling

  As I´m starting from scratch, let´s build the ERD (Entity-Relationship Diagram). I´m using brModelo to do it. You can check it out here https://github.com/chcandido/brModelo

Knowing that 
<pre>
1)users and brokers must have phones and addresses and 
2)Users can have only one broker, we can build the model with this cardinality:
</pre>
![image](https://github.com/ricauduro/modeling_etl_display/assets/58055908/4c5c1b63-233a-4d6d-b4ce-0fd214d73496)

Our conceptual model looks like this

![image](https://github.com/ricauduro/modeling_etl_display/assets/58055908/dbf5cf17-c45d-43cc-b706-dd8721e5583b)

Note that on the "N" side of the relationship there is always a field that points to the primary key on the "1" side.
This is a characteristic of 1:N relationships. In the table on the "N" side, a "foreign key" is created that points to the primary key of the "1" table.

https://www.devmedia.com.br/modelagem-1-n-ou-n-n/38894

with our conceptual model ready, we can build move to our logical model

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

at code´s end we have this funciton, also imported from shared_code, that will create a Data Factory pipeline run, to start Databricks transformations

```python
        call_pipeline(c_id, c_secret, t_id, s_id, rg_name, df_name, p_name)
```
this is the pipeline

![image](https://github.com/ricauduro/modeling_etl_display/assets/58055908/04a991ca-e36d-49a7-b7fc-bdaf9b6e67d0)


With Lookup activity it´ll get the max date from SQL table 

![image](https://github.com/ricauduro/modeling_etl_display/assets/58055908/33d97eb2-2762-4231-b985-97dd3253888f)

and it´s passing this value to Databricks notebook

![image](https://github.com/ricauduro/modeling_etl_display/assets/58055908/67595a16-db7c-45d8-ad41-3bd7894584ad)

so when reading API data, which are all togheter inside the container, we can use the date to filter and process only newest data

### Databricks

My Databricks notebook contain some links to usefull info, like create a secret scope, adding system variables and accessing them, mount a blob... you´ll thes config to access you data without exposing sensitive data and doing it in the safe way 

![image](https://github.com/ricauduro/modeling_etl_display/assets/58055908/e5bada2e-8cc5-46d5-b0b1-e29046c46cee)

but I´ll focus only on data transformation and saving it as a table into SQL DB.

At our AzFunction, we were storing data as a json object, so we´ll use json method to start reading the data. 

```python
df = spark.read.json("dbfs:/mnt/blob/dax*")
```
our display will be this one
![image](https://github.com/ricauduro/modeling_etl_display/assets/58055908/67d7e5ce-7898-423e-8f40-189780459de0)

Now that we know what´s is being ingested, we can start some transformations, first exploding fields
```python
df_exp = df.select(
    'data.symbol',
    'data.ask',
    'data.baseVolume24h',
    'data.bid',
    'data.high24h',
    'data.lastPrice',
    'data.low24h',
    'data.open24h',
    'data.quoteVolume24h',
    'data.timestamp'

```
![image](https://github.com/ricauduro/modeling_etl_display/assets/58055908/d47b356b-c7e2-4396-82e3-382aa6671775)

We can see that timestamp column need some transformation also... I´m taking advantage to add some id´s that we´ll use with other tables.

![image](https://github.com/ricauduro/modeling_etl_display/assets/58055908/d9aac1cb-5996-43f1-8361-5940bf425298)


We read all data in the storage with 
```python
df = spark.read.json("dbfs:/mnt/blob/dax*")
```
so our DF have all data stored in the blob. Now we´ll use the date that we get from SQL DB with Data Factory Lookup activity and we´ll use it to filter DF´s data to have only data that are not in the SQL DB

```python
data = dbutils.widgets.get("max_date")
df = df.filter(col('timestamp') > data) if data is not None else df
```

with the transformations and filters apllied, we can save the DF as a tabe at SQL DB. 


```python
jdbcDriver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'

jdbcUrl = 'jdbc:sqlserver://{0}:{1};database={2}'.format(server, 1433, db)
connectionProperties = {
  'user' : sql_user,
  'password' : sql_secret,
  'driver' : jdbcDriver
}

print(jdbcUrl)
table = 'dax_api'

(
  df.write.mode('append')
  .format('jdbc')
  .option('url', jdbcUrl)
  .option('dbtable', table)
  .option('user', sql_user)
  .option('password', sql_secret)
  .option('driver', jdbcDriver)
  .save()
)
```
sql_user, sql_secret, jdbcDriver and other params were added to cluster session, and we´re accessing those values with this code, so we can protect sensitive data

```python
sql_user = spark.conf.get("spark.sql_user")
sql_secret = spark.conf.get("spark.sql_secret")
server = spark.conf.get("server")
db = spark.conf.get("db")
```

Here are some usefull links to set up databricks
https://learn.microsoft.com/en-us/azure/data-factory/transform-data-using-databricks-notebook

https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes -> Grant Databricks KV access (this one is in conflict with grant AzFunction KV access... use carefully)

Now that all tables are in place, let´s start to build our fact and dimensions


### Dimensional Modeling
