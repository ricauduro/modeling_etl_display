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

Knowing that 1)users and brokers must have phones and addresses and 2)Users can have only one broker, we can build the model with this cardinality:

![image](https://github.com/ricauduro/modeling_etl_display/assets/58055908/4c5c1b63-233a-4d6d-b4ce-0fd214d73496)

Our conceptual model looks like this

![image](https://github.com/ricauduro/modeling_etl_display/assets/58055908/dd05a0c8-adf3-457c-aebd-24dbe1fee505)


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

INSERT INTO USUARIO (ENDERECO_ID, CORRETORA_ID, NOME, SOBRENOME, DATA_NASCIMENTO)
VALUES (1, 1, 'Ricardo', 'Cauduro', '1983-02-25');

INSERT INTO USUARIO (ENDERECO_ID, CORRETORA_ID, NOME, NOME_MEIO, SOBRENOME, DATA_NASCIMENTO)
VALUES (1, 1, 'Rita', 'de Cassia', 'Martins Cauduro', '1986-04-30');

INSERT INTO TELEFONE (USUARIO_ID, CORRETORA_ID, TIPO, DDD, NUMERO)
VALUES (1, 1, 'Celular', 19, 987654321),
       (2, 1, 'Celular', 19, 978644822);
```
