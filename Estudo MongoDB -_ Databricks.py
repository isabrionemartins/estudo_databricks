# Databricks notebook source
# MAGIC %md # Introdução
# MAGIC Estudo de importação de dados de um banco MongoDB para o Databricks

# COMMAND ----------

# MAGIC %md # Dependencias
# MAGIC
# MAGIC Vamos utilizar nesse projeto as seguintes bibliotecas:
# MAGIC
# MAGIC - pymongo
# MAGIC - pyspark
# MAGIC - json
# MAGIC
# MAGIC O Pymongo será utilizado para acessar e extrair os dados do MongoDB Cloud.
# MAGIC
# MAGIC O Pyspark será utilizado para tratar e manipular os dados extraídos do Mongo. Também vamos utilizar ele para criar tabelas
# MAGIC
# MAGIC O Json será utilizado para carregar o arquivo de configuração de acesso do Mongo que foi enviado para o DBFS.

# COMMAND ----------

!python -m pip install pymongo

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DateType, FloatType
from pyspark.sql.functions import explode
from pymongo.mongo_client import MongoClient
from json import loads

# COMMAND ----------

# MAGIC %md ### Carregamento dos Acessos do Mongo
# MAGIC O Databricks Community tem algumas limitações, e uma delas é a falta do Secrets dentro do ambiente. Por conta disso, podemos utilizar uma alternativa que atenderia o caso atual, que é enviar o arquivo para o DBFS e pegar as informações por lá, mas quando tentamos usar o "with open(path) as file" do python ele não consegue encontrar o arquivo. Neste caso precisamos de uma solução alternativa que vai resolver este caso. Sabendo que o arquivo com os acessos é bem pequeno, podemos utilizar o comando "dbutils.fs.head" para puxar o inicio do arquivo (que neste caso carregaria todo o arquivo), assim podemos usar a biblioteca json para carregar os dados como dicionário dentro do python.

# COMMAND ----------

mongo_access = dbutils.fs.head("dbfs:/FileStore/shared_uploads/isabrionemartins2030@gmail.com/mongo_access.json")
mongo_access = loads(mongo_access)

# COMMAND ----------

username = mongo_access["username"]
password = mongo_access["password"]
host = mongo_access["host"]
# Com essas informações é possível contruir a url de acesso do mongo (essa estrutura é informada pelo próprio MongoDB Cloud)
client = MongoClient(f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority")

# COMMAND ----------

# MAGIC %md # Exploração
# MAGIC Para carregar os dados para dentro do databricks é necessário entender quais databases e collections vamos extrair do Mongo. Como é um projeto estudo, poderiamos pegar tudo isso do MongoDB Cloud. Mas vamos utilizar todos os passos que seriam utilizados em um projeto real. 
# MAGIC
# MAGIC No projeto, vamos procurar a Database "sample_restaurants". Nela precisamos encontrar a collection "restaurants" que possui todos os dados e informações de vários restaurantes.

# COMMAND ----------

database = "sample_restaurants"
databases_names = client.list_database_names()
# Confirmação que o database existe de fato dentro do MongoDB
print(database in databases_names)

# COMMAND ----------

collection = "restaurants"
collections_name = client[database].list_collection_names()
# Confirmação que a collection existe de fato dentro do database
print(collection in collections_name)

# COMMAND ----------

# Para puxarmos todos os dados do MongoDB, informamos dentro do método find um dicionário vazio. Caso precise ser feito algum filtro dentro dele, podemos passar ali dentro a lógica do filtro
data = client[database][collection].find({})
# Podemos mostrar 1 dado da lista para vermos a sua estrutura.
print(data[0])

# COMMAND ----------

# MAGIC %md # Transformação
# MAGIC Uma vez que entendemos qual é a estrutura do dado da collection, podemos criar um processo para ajustar o que é necessário. No nosso caso, o único dado que precisa ser ajustado é o "_id". Este dado vem como um ObjectId, e por conta de ser um objeto de uma classe do pymongo o pyspark não vai conseguir converter. Já que estamos lidando com pouco dado, podemos converter esse dado usando um for do python. Converter esse dado para str já é o suficiente para conseguirmos apenas o ID do campo, sem ser um objeto.

# COMMAND ----------

transformed_data = []
for row in data:
    transformed_data.append({
        "id": str(row["_id"]),
        "address": row["address"],
        "borough": row["borough"],
        "cuisine": row["cuisine"],
        "grades": row["grades"],
        "name": row["name"],
        "restaurant_id": row["restaurant_id"],
    })
# Após a transformação, podemos mostrar o mesmo dado que usamos como exemplo para ver se está tudo certo
print(transformed_data[0])

# COMMAND ----------

# MAGIC %md # Definição do schema
# MAGIC Após tratar o dado, podemos seguir com a definição de como será a tabela no spark. Como estamos trabalhando com dados JSON, precisamos utilizar um processo do spark que é o schema. Isto significa que precisamos definir quais são os campos que vamos enviar dentro de cada dicionário e o seu tipo primitivo. Podemos utilizar essa lista como referencia de qual é o tipo do python e qual é o tipo no pyspark.
# MAGIC
# MAGIC https://spark.apache.org/docs/latest/sql-ref-datatypes.html

# COMMAND ----------

schema = StructType([
    StructField("id", StringType(), True),
    StructField("address", StructType([
        StructField("building", StringType(), True),
        StructField("coord", ArrayType(FloatType()), True),
        StructField("street", StringType(), True),
        StructField("zipcode", StringType(), True),
    ]), True),
    StructField("borough", StringType(), True),
    StructField("cuisine", StringType(), True),
    StructField("grades", ArrayType(
        StructType([
            StructField("date", DateType(), True),
            StructField("grade", StringType(), True),
            StructField("score", IntegerType(), True),
        ])
    ), True),
    StructField("name", StringType(), True),
    StructField("restaurant_id", StringType(), True)
])

# COMMAND ----------

# MAGIC %md # Criação do dataframe Spark
# MAGIC Com a transformação feita e o schema definido, podemos agora criar o dataframe Spark sem nenhum erro.

# COMMAND ----------

df = spark.createDataFrame(data=transformed_data, schema=schema)
df.display()

# COMMAND ----------

# MAGIC %md # Criação da tabela RAW
# MAGIC Com o df spark pronto podemos criar a tabela que possuirá os dados do ETL. No caso do pyspark, podemos criar de uma maneira super fácil e automática usando o "saveAsTable". Ele utilizara a estrutura do df que foi definida no schema para criar os campos da tabela, e no final enviará todos os dados do df para a tabela.
# MAGIC
# MAGIC Para facilitar o entendimento das tabelas, vamos utilizar como regra o seguinte:
# MAGIC
# MAGIC - Tabelas que possuem o dado original do MongoDB possuirá o prefixo "RAW"
# MAGIC - Tabelas que possuem o dado transformado da tabela "RAW" porém não agregados possuirá o prefixo "INT"
# MAGIC - Tabelas que possuem o dado transformado e agregados das tabelas "RAW" e/ou "INT" possuirá o prefixo "EXP"

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("raw_restaurantes")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirmamos que a criação da tabela deu certo. Vamos usar um select com apenas 10 linhas da tabela
# MAGIC select * from raw_restaurantes limit 10

# COMMAND ----------

# MAGIC %md # Criação da tabela INT
# MAGIC Após a tabela RAW estar pronta, vamos criar a nossa tabela INT, que será uma tabela com os dados de notas dos restaurantes.
# MAGIC
# MAGIC Na tabela RAW, esses dados estão dentro da coluna "grades" que é uma lista de notas. Porém para facilitar a utilização delas pelos analistas e também na exp, vamos fazer com que cada nota dentro do grades seja uma linha, repetindo todos os outros dados do restaurantes.
# MAGIC
# MAGIC Esse processo precisa de um comando do pyspark chamado "explode". Ele quebra listas e permite que você acesso os dados como se fossem dicionários.

# COMMAND ----------

# Primeiro puxamos todos os dados da tabela raw
restaurantes = sql("select * from raw_restaurantes")
# Selecionamos os campos que vamos utilizar, e já aproveitamos e fazemos o explode do campo "grades". No fim renomeamos o campo explodido que é nomeado como "col" para novamente "grades".
restaurantes_exploded = restaurantes.select(
    restaurantes.borough, 
    restaurantes.cuisine, 
    restaurantes.name, 
    explode(restaurantes.grades)
).withColumnRenamed(
    "col", "grades"
)
# Selecionamos novamente os campos que vamos utilizar e acessamos os campos que eram da lista do "grades". Estes campos são o "date", "grade" e "score". No fim renomeamos cada campo que vem com "grades." para apenas o nome simplificado do campo.
restaurantes_notas = restaurantes_exploded.select(
    restaurantes_exploded.borough, 
    restaurantes_exploded.cuisine, 
    restaurantes_exploded.name,
    restaurantes_exploded.grades.date,
    restaurantes_exploded.grades.grade,
    restaurantes_exploded.grades.score
).withColumnRenamed(
    "grades.date", "date"
).withColumnRenamed(
    "grades.grade", "grade"
).withColumnRenamed(
    "grades.score", "score"
)
# Exibimos o dataframe para ver se tudo esta como planejado.
restaurantes_notas.display()

# COMMAND ----------

# Geramos a tabela usando a mesma lógica que fizemos com o RAW
restaurantes_notas.write.mode("overwrite").saveAsTable("int_restaurantes_notas")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirmamos que a criação da tabela deu certo. Vamos usar um select com apenas 10 linhas da tabela
# MAGIC select * from int_restaurantes_notas limit 10

# COMMAND ----------

# MAGIC %md # Criação das tabelas EXPs
# MAGIC Após a tabela RAW e INT estarem prontas, vamos criar as nossas tabelas EXPs. Vamos criar duas tabelas EXP. 
# MAGIC
# MAGIC Uma será uma agregação dos campos "cuisine" que nos diz qual é o tipo da cozinha, e o outro é o campo "borough" que nos diz qual é o bairro que a cozinha está. Ao fim, vamos contabilizar quantos restaurantes tem em cada bairro e de cada tipo.
# MAGIC
# MAGIC A outra será uma agregação das notas dos restaurantes. Para esse caso vamos considerar que o "score" se refere a nota de 0 a 10 do restaurante. Vamos fazer uma média de nota de cada restaurante.

# COMMAND ----------

# MAGIC %md ## Primeira EXP

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aqui vamos utilizar o "group by" para agrupar todos os dados que possuem o mesmo "cuisine" e "borough". Para poder haver o agrupamento, precisamos informar qual o tipo de agregação que será feito, no nosso caso, uma contagem de cada linha que possui os mesmos valores desses campos, o "count"
# MAGIC select cuisine as tipo_cozinha, borough as bairro, count(cuisine) as total from raw_restaurantes group by cuisine, borough

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Neste caso vamos utilizar uma outra forma de criar tabela, que é utilizando o código sql "create or replace table" com base no código select.
# MAGIC create or replace table exp_restaurantes_tipos_e_bairros as
# MAGIC select cuisine as tipo_cozinha, borough as bairro, count(cuisine) as total from raw_restaurantes group by cuisine, borough

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirmamos que a criação da tabela deu certo. Vamos usar um select com apenas 10 linhas da tabela
# MAGIC select * from exp_restaurantes_tipos_e_bairros limit 10

# COMMAND ----------

# MAGIC %md ## Segunda EXP

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Agrupamos os dados pelo nome do restaurante, e fazemos uma média (avg) das notas (score). Porém para ficar melhor de ver o valor das notas, convertemos o dado para "decimal" com 3 casas antes da vírgula e 2 após
# MAGIC select name as restaurante, avg(score)::decimal(2,2) as total from int_restaurantes_notas group by name order by total desc

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Para finalizar este estudo vamos criar essa exp de uma forma diferente. Em vez de uma tabela, vamos criar uma view. Este tipo de armazenamento funciona parecido com uma tabela, porém em vez dele armazenar os dados, ele armazena a consulta e deixa ela otimizada para quem for utilizar. Podemos criar a view da mesma forma que criamos a outra exp, com o "create or replace view".
# MAGIC create or replace view exp_restaurantes_media_notas as
# MAGIC select name as restaurante, avg(score)::decimal(2,2) as total from int_restaurantes_notas group by name order by total desc

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirmamos que a criação da view deu certo. Vamos usar um select com apenas 10 linhas da tabela
# MAGIC select * from exp_restaurantes_media_notas limit 10

# COMMAND ----------


