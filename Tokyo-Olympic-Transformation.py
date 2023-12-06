# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "b221451a-bc31-460e-b9a1-937727236f82",
"fs.azure.account.oauth2.client.secret": 'R0T8Q~GAB34xDNEtb9N4X0AszBMYJTyx-jqPoa4O',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/acbacd57-46cc-4507-a5af-77fc842ba10a/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
source = "abfss://tokyoolympicdata@tokyoolympicreha.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolymic",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolymic/raw_data"

# COMMAND ----------

athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw_data/atheletes.csv")
athletes=athletes.withColumnRenamed("PersonName","Person_Name") #Rename column
athletes.show()

# COMMAND ----------


coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw_data/coaches.csv")
coaches.show()

# COMMAND ----------

entriesgender = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw_data/entriesgender.csv")
entriesgender.show()

# COMMAND ----------

medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw_data/medals.csv")
medals=medals.withColumnRenamed("Rank By Total","Rank_By_Total")
medals.show()


# COMMAND ----------

teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw_data/teams.csv")
teams.show()
