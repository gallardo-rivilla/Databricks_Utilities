# Databricks notebook source
# MAGIC %md
# MAGIC # Conversión de Parquet a Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC - Un Delta Lake tiene varias ventajas sobre una tabla de Parquet simple, como la compatibilidad con transacciones ACID, viajes en el tiempo y control de concurrencia, así como optimizaciones para mejorar el rendimiento de las consultas. 
# MAGIC - Puede aprovechar fácilmente estas características convirtiendo su mesa de parquet en una Delta Lake. 
# MAGIC - El código es simple y no es necesario volver a escribir los archivos de Parquet, por lo que requiere menos recursos computacionales de los que podría imaginar.

# COMMAND ----------

# MAGIC %md
# MAGIC Más información: https://delta.io/blog/2022-09-23-convert-parquet-to-delta/

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Parquet a Delta Lake API🤔

# COMMAND ----------

# MAGIC %md
# MAGIC - Delta Lake proporciona una API, `DeltaTable.convertToDelta` para convertir una tabla de Parquet en un Delta Lake. 
# MAGIC 
# MAGIC - Por ejemplo, podemos usar el siguiente código para convertir una tabla de Parquet sin particiones en un Delta Lake usando PySpark:

# COMMAND ----------

# MAGIC %md
# MAGIC `from delta.tables import *`
# MAGIC 
# MAGIC `deltaTable = DeltaTable.convertToDelta(spark, "parquet.``<path-to-table>`")`

# COMMAND ----------

# MAGIC %md
# MAGIC - Vamos a crear un conjunto de datos de Parquet y ejecutar este comando en un conjunto real de archivos. 
# MAGIC - Comenzaremos creando una tabla de Parquet con tres filas de datos:

# COMMAND ----------

# Creando una tabla de Parquet 
columns = ["language", "num_speakers"]
data = [("English", "1.5"), ("Mandarin", "1.1"), ("Hindi", "0.6")]
rdd = spark.sparkContext.parallelize(data)
df = rdd.toDF(columns)

df.write.format("parquet").save("/mnt/demo/schema/lake1")

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora ejecutemos el código para convertir la mesa Parquet en un lago Delta:

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.convertToDelta(spark, "parquet.`/mnt/demo/schema/lake1`")

# COMMAND ----------

# MAGIC %md
# MAGIC - El comando escanea todos los archivos de Parquet y crea el `_delta_log` directorio, que contiene los metadatos necesarios para las consultas de datos de Delta Lake. 
# MAGIC - Tenga en cuenta que todos los archivos de Parquet en Delta Lake son los mismos que los archivos de Parquet en la tabla de Parquet. 
# MAGIC - A pesar de los metadatos agregados, la conversión de Parquet a Delta Lake genera solo un pequeño aumento en los costos de almacenamiento porque no se reescriben los datos.
# MAGIC - Las funciones adicionales a las que puede acceder definitivamente valen la pena el pequeño aumento en el costo.

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Convertir una tabla parquet particionada a Delta Lake🎡

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora veamos el proceso de convertir una mesa de parquet dividida en un Delta Lake. Comenzaremos creando la tabla:

# COMMAND ----------

df.write.partitionBy("language").format("parquet").save("/mnt/demo/schema/lake2")

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora intentemos convertir esta mesa de parquet en un Delta Lake:

# COMMAND ----------

deltaTable = DeltaTable.convertToDelta(spark, "parquet.`/mnt/demo/schema/lake2`")

# COMMAND ----------

# MAGIC %md
# MAGIC Sin embargo, hay un problema: este código genera un error.

# COMMAND ----------

# MAGIC %md
# MAGIC - En una tabla de Parquet, los tipos de datos de las columnas de partición están determinados por los nombres de los directorios, que pueden ser ambiguos. 
# MAGIC -Por ejemplo, cuando lee el nombre del directorio date=2022-09-21, Delta Lake no tiene forma de saber qué tipo de datos para la columna de partición de fecha es el deseado: ¿debería ser cadena, fecha, marca de tiempo? 
# MAGIC - Por lo tanto, debe proporcionar un tercer parámetro: una cadena con formato DDL de Hive que especifique los nombres de las columnas y los tipos de datos de las particiones. Por ejemplo:

# COMMAND ----------

deltaTable = DeltaTable.convertToDelta(spark, "parquet.`/mnt/demo/schema/lake2`", "language STRING")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Pros y contras de convertirse a Delta Lakes

# COMMAND ----------

# MAGIC %md
# MAGIC - Un Delta Lake tiene múltiples ventajas sobre una mesa de parquet simple: permite viajar en el tiempo entre diferentes versiones de sus datos, transacciones ACID, seguridad de concurrencia y una variedad de otros beneficios. 
# MAGIC 
# MAGIC - La conversión a un Delta Lake es rápida y fácil, y casi no tiene inconvenientes.
# MAGIC 
# MAGIC - Una cosa a tener en cuenta es que la conversión de una tabla de Parquet a Delta Lake puede ser costosa desde el punto de vista computacional cuando hay muchos archivos de Parquet. 
# MAGIC 
# MAGIC - Esto se debe a que el proceso de conversión necesita abrir todos los archivos y calcular las estadísticas de metadatos para crear el archivo _delta_log.
# MAGIC 
# MAGIC - Además, una vez que una tabla de Parquet se convierte en Delta Lake, solo la pueden leer los motores de consulta que tienen un lector de Delta Lake.
