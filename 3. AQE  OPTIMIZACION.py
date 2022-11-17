# Databricks notebook source
# MAGIC %md
# MAGIC # Optimización del rendimiento de Spark con la ejecución de consultas adaptable

# COMMAND ----------

# MAGIC %md
# MAGIC - El rendimiento, la capacidad de mantenimiento y la escalabilidad de un entorno Data Lakehouse listo para la producción es lo que realmente determina su éxito general. Los sistemas de almacenamiento de datos SQL maduros tradicionales vienen con los beneficios de la indexación, las estadísticas, las optimizaciones automáticas del plan de consultas y mucho más.
# MAGIC - El concepto de Data Lakehouse está madurando lenta pero seguramente sus capacidades y características en comparación con muchos de estos sistemas tradicionales. 
# MAGIC - `Adaptive Query Execution (AQE)` es una de esas características que ofrece Databricks para acelerar una consulta Spark SQL en tiempo de ejecución. En este artículo, demostraré cómo comenzar a comparar el rendimiento de AQE que está deshabilitado versus habilitado al consultar cargas de trabajo de big data en su Data Lakehouse.

# COMMAND ----------

from pyspark.sql.functions import *

Data = "/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-*"

SchemaDF = spark.read.format("csv")   \
            .option("header", "true")  \
            .option("inferSchema", "true") \
            .load("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-02.csv.gz")



# COMMAND ----------

nyctaxiDF = spark.read.format("csv")\
.option("header", "true")\
.schema(SchemaDF.schema)\
.load(Data)

# COMMAND ----------

nyctaxiDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.  Parte 1: Comparación del rendimiento de AQE en consultas sin uniones

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM nyctaxi

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deshabilitar AQE
# MAGIC - Para probar el rendimiento de AQE desactivado, continúe y ejecute el siguiente comando para set `spark.sql.adaptive.enabled = false`.
# MAGIC - Esto asegurará que AQE se apague para esta prueba de rendimiento en particular.

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.conf.set("spark.sql.adaptive.enabled",false)
# MAGIC spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",true)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT VendorID, SUM(total_amount) as sum_total
# MAGIC FROM nyctaxi
# MAGIC GROUP BY VendorID
# MAGIC ORDER BY sum_total DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Habilitar AQE
# MAGIC 
# MAGIC - A continuación, continúe y habilite AQE configurándolo en verdadero con el siguiente comando: `set spark.sql.adaptive.enabled = true`. 
# MAGIC - En esta sección, ejecutará la misma consulta proporcionada en la sección anterior para medir el rendimiento del tiempo de ejecución de la consulta con AQE habilitado.

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.conf.set("spark.sql.adaptive.enabled",true)
# MAGIC spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",true)

# COMMAND ----------

# MAGIC %md
# MAGIC Una vez más, continúe y ejecute la misma consulta nuevamente y observe el tiempo de ejecucion

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT VendorID, SUM(total_amount) as sum_total
# MAGIC FROM nyctaxi
# MAGIC GROUP BY VendorID
# MAGIC ORDER BY sum_total DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Parte 2: Comparación del rendimiento de AQE en consultas con combinaciones

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deshabilitar AQE
# MAGIC - Para probar el rendimiento de AQE desactivado, continúe y ejecute el siguiente comando para set `spark.sql.adaptive.enabled = false`.
# MAGIC - Esto asegurará que AQE se apague para esta prueba de rendimiento en particular.

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.conf.set("spark.sql.adaptive.enabled",false)
# MAGIC spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",true)

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT a.VendorID, SUM(a.total_amount) as sum_total
# MAGIC FROM nyctaxi a
# MAGIC WHERE tpep_pickup_datetime BETWEEN '2019-05-01 00:00:00' AND '2019-05-03 00:00:00'
# MAGIC GROUP BY VendorID
# MAGIC ORDER BY sum_total DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.VendorID, SUM(a.total_amount) as sum_total
# MAGIC FROM nyctaxi a
# MAGIC WHERE tpep_pickup_datetime BETWEEN '2019-05-01 00:00:00' AND '2019-05-03 00:00:00'
# MAGIC GROUP BY VendorID
# MAGIC ORDER BY sum_total DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Habilitar AQE
# MAGIC 
# MAGIC - A continuación, continúe y habilite AQE configurándolo en verdadero con el siguiente comando: `set spark.sql.adaptive.enabled = true`. 
# MAGIC - En esta sección, ejecutará la misma consulta proporcionada en la sección anterior para medir el rendimiento del tiempo de ejecución de la consulta con AQE habilitado.

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.conf.set("spark.sql.adaptive.enabled",true)
# MAGIC spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",true)

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT a.VendorID, SUM(a.total_amount) as sum_total
# MAGIC FROM nyctaxi a
# MAGIC WHERE tpep_pickup_datetime BETWEEN '2019-05-01 00:00:00' AND '2019-05-03 00:00:00'
# MAGIC GROUP BY VendorID
# MAGIC ORDER BY sum_total DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.VendorID, SUM(a.total_amount) as sum_total
# MAGIC FROM nyctaxi a
# MAGIC WHERE tpep_pickup_datetime BETWEEN '2019-05-01 00:00:00' AND '2019-05-03 00:00:00'
# MAGIC GROUP BY VendorID
# MAGIC ORDER BY sum_total DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://www.mssqltips.com/tipimages2/6983_optimizing-spark-performance-adaptive-query-execution.021.png" alt="AQE" style="width: 600px">

# COMMAND ----------


