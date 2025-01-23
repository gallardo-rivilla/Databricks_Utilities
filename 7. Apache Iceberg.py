# Databricks notebook source
# MAGIC %md
# MAGIC # 🧊  Apache Iceberg 👉Un formato para gobernarlos a todos {🎆LakeHouse🎆} [2024/25]

# COMMAND ----------

# MAGIC %md
# MAGIC ![](https://gallardorivilla.es/wp-content/uploads/2024/12/APACHE-ICEBERG.png)

# COMMAND ----------

df = spark.table("udemy.udemy_reviews")
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

# Disable Deletion Vectors on the table
spark.sql("""
ALTER TABLE example.udemy.udemy_reviews
SET TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'false'
)
""")

# Run REORG PURGE command to purge the Deletion Vectors
spark.sql("""
REORG TABLE example.udemy.udemy_reviews APPLY (PURGE)
""")

# Set the desired table properties
spark.sql("""
ALTER TABLE example.udemy.udemy_reviews
SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES  example.udemy.udemy_reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED example.udemy.udemy_reviews

# COMMAND ----------

# MAGIC %md
# MAGIC Utilice la siguiente sintaxis para activar manualmente la generación de metadatos de Iceberg:
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MSCK REPAIR TABLE example.udemy.udemy_reviews SYNC METADATA 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Actualizar los datos de la tabla Iceberg
# MAGIC
# MAGIC Realice actualizaciones en registros específicos para ver cómo Iceberg maneja los cambios:

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE example.udemy.udemy_reviews 
# MAGIC SET `Rating` = 5
# MAGIC WHERE `Course_Name` = 'Data Engineering using Databricks on AWS and Azure';

# COMMAND ----------

# Listar todos los puntos de montaje en Databricks
mounts = dbutils.fs.ls("/mnt")
for mount in mounts:
    print(f"Mount Point: {mount.path}")
