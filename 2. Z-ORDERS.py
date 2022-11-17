# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Ajuste del rendimiento de Apache Spark con Z-ORDERS✨

# COMMAND ----------

# MAGIC %md
# MAGIC - Al consultar terabytes o petabytes de big data para análisis con Apache Spark, es fundamental tener velocidades de consulta optimizadas. 
# MAGIC - Hay algunos comandos de optimización disponibles dentro de Databricks que se pueden usar para acelerar las consultas y hacerlas más eficientes. 
# MAGIC -  Al ver que Z-ORDERS y la omisión de datos son características de optimización que están disponibles en Databricks, 
# MAGIC 
# MAGIC  `¿Cómo podemos comenzar a probarlas y usarlas en Databricks Notebooks?`

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://cms.databricks.com/sites/default/files/inline-images/optimize_where.jpg" alt="Z-ORDERS" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Mas información: https://www.databricks.com/blog/2018/07/31/processing-petabytes-of-data-in-seconds-with-databricks-delta.html

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://www.databricks.com/wp-content/uploads/2018/07/Screen-Shot-2018-07-30-at-2.03.55-PM.png" alt="Z-ORDERS">

# COMMAND ----------

# MAGIC %md
# MAGIC - El comando `OPTIMIZE` puede lograr esta compactación por sí solo sin Z-Ordering, sin embargo, `Z-Ordering` nos permite especificar la columna para compactar y optimizar, lo que afectará las velocidades de consulta si la columna especificada está en una cláusula Where y tiene una alta cardinalidad. 
# MAGIC - Además, la omisión de datos es una característica automática del comando de optimización y funciona bien cuando se combina con Z-Ordering.

# COMMAND ----------

flights = spark.read.format("csv")   \
        .option("header", "true")  \
        .option("inferSchema", "true")   \
        .load("/databricks-datasets/asa/airlines/2008.csv")


# COMMAND ----------

display(flights)

# COMMAND ----------

flights.count()

# COMMAND ----------

# Comprobamos en databricks las unidades montadas:
display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC Guardamos el Spark Dataframe en formato delta y lo particionamos por columna origen

# COMMAND ----------

(
  flights
  .write
  .partitionBy("Origin")
  .format("delta")
  .mode("overwrite")
  .save("/mnt/demo/flights/flights_delta")
)

# COMMAND ----------

# MAGIC %md
# MAGIC - Una vez que el comando termina de ejecutarse, podemos ver en la imagen a continuación que los datos de vuelo se han particionado y persistido en ADLS gen2. **Hay más de 300 carpetas particionadas por 'Origen'.**
# MAGIC - Al navegar a delta_log, podemos ver los archivos de registro iniciales, representados principalmente por los archivos *.json.
# MAGIC - Después de descargar el archivo json delta_log inicial y abrirlo, podemos ver que se agregaron más de 2310 archivos nuevos a las más de 300 carpetas.

# COMMAND ----------

# MAGIC %md
# MAGIC # Ahora creamos una tabla delta con los ficheros que hemos persisitido en el DataLake

# COMMAND ----------

spark.sql("CREATE TABLE flights USING DELTA LOCATION '/mnt/demo/flights/flights_delta'")


# COMMAND ----------

# MAGIC %md
# MAGIC - Después de crear la tabla de Hive,verificamos el número total de registros.
# MAGIC - Como podemos ver, este es un conjunto de datos bastante grande con **más de 7 millones de registros.**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Count(*) FROM flights

# COMMAND ----------

# MAGIC %md
# MAGIC - A continuación, podemos ejecutar una consulta más compleja que aplicará un filtro a la tabla de vuelos en una columna sin particiones, Día del mes. 
# MAGIC - En la visualización de resultados en la imagen a continuación, podemos ver que la consulta tardó más de 2 minutos en completarse. 
# MAGIC - Este tiempo nos permite establecer el punto de referencia inicial para el tiempo de comparación después de ejecutar el comando Z-Order.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) as Flights, Dest, Month 
# MAGIC FROM flights 
# MAGIC WHERE DayofMonth = 5 
# MAGIC GROUP BY Month, Dest

# COMMAND ----------

# MAGIC %md
# MAGIC - A continuación, podemos ejecutar el siguiente comando `OPTIMIZE` combinado con el comando `Z-ORDER` en la columna que queremos filtrar, que es `DayofMonth`. 
# MAGIC - Tenga en cuenta que las optimizaciones de orden Z funcionan mejor en columnas que tienen una **alta cardinalidad.**
# MAGIC - Según los resultados, se eliminaron 2308 archivos y se agregaron 300 archivos como parte del proceso Z-ORDER OPTIMIZE.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE flights ZORDER BY (DayofMonth)

# COMMAND ----------

# MAGIC %md
# MAGIC Dentro del **delta_log**, ahora hay un nuevo archivo json que podemos descargar y abrir para revisar los resultados.

# COMMAND ----------

# MAGIC %md
# MAGIC Como era de esperar, podemos ver las acciones realizadas en estos registros en función de las líneas de eliminación y adición en el archivo json.

# COMMAND ----------

# MAGIC %md
# MAGIC - Como información adicional, al navegar dentro de una de las carpetas de partición, se ha creado un nuevo archivo. 
# MAGIC - Si bien el comando Orden Z puede personalizar el tamaño de compactación, generalmente apunta a alrededor de 1 GB por archivo cuando es posible.

# COMMAND ----------

# MAGIC %md
# MAGIC # Ahora ejecutamos de nuevo la misma consulta y vemos lo que tarda:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) as Flights, Dest, Month 
# MAGIC FROM flights 
# MAGIC WHERE DayofMonth = 5 
# MAGIC GROUP BY Month, Dest

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comprobamos que tarda unos `8.72 segundos` con respecto a los `41.56 segundos` de la primera ejecución sin Z-ORDERS

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Data Skipping🧨

# COMMAND ----------

Data = "/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-*"

SchemaDF = spark.read.format("csv")   \
            .option("header", "true")  \
            .option("inferSchema", "true") \
            .load("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-02.csv.gz")



# COMMAND ----------

# MAGIC %md
# MAGIC Creamos el Spark Dataframe

# COMMAND ----------

nyctaxiDF = spark.read.format("csv")\
.option("header", "true")\
.schema(SchemaDF.schema)\
.load(Data)

# COMMAND ----------

nyctaxiDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC - Este  bloque de código agregará algunos campos de partición al Spark Dataframe existente por Año, Año_Mes y Año_Mes_Día. 
# MAGIC - Estas columnas adicionales se basarán en la marca de fecha y hora y ayudarán con la partición, la omisión de datos, el Z-ORDER y, en última instancia, velocidades de consulta más eficaces.

# COMMAND ----------

from pyspark.sql.functions import *
nyctaxiDF = nyctaxiDF.withColumn('Year', year(col("tpep_pickup_datetime")))
nyctaxiDF = nyctaxiDF.withColumn('Year_Month', date_format(col("tpep_pickup_datetime"),"yyyyMM"))
nyctaxiDF = nyctaxiDF.withColumn('Year_Month_Day', date_format(col("tpep_pickup_datetime"),"yyyyMMdd"))

# COMMAND ----------

display(nyctaxiDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Guardamos el Spark Dataframe en formato delta y lo particionamos por columna origen

# COMMAND ----------

(
nyctaxiDF
 .write
 .partitionBy("Year")
 .format("delta")
 .mode("overwrite")
 .save("/mnt/demo/nyctaxi/nyctaxi_delta")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Como podemos ver en el archivo json delta_log, se agregaron ~400 archivos nuevos.

# COMMAND ----------

# MAGIC %md
# MAGIC A continuación, podemos crear una tabla de Hive utilizando la ruta delta de ADLS2.

# COMMAND ----------

spark.sql("CREATE TABLE nyctaxi USING DELTA LOCATION '/mnt/demo/nyctaxi/nyctaxi_delta'")


# COMMAND ----------

# MAGIC %md
# MAGIC Podemos ver que la tabla Delta tiene mas de 84 millones de registros

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM nyctaxi

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ahora es el momento de aplicar Z-Ordering a la tabla en Year_Month_Day ejecutando el siguiente código SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE nyctaxi 
# MAGIC ZORDER BY (Year_Month_Day)

# COMMAND ----------

# MAGIC %md
# MAGIC Como podemos ver en la imagen a continuación, se eliminaron los 400 archivos y solo se agregaron 10 archivos nuevos. Además, tenga en cuenta que se trata de una eliminación y adición lógica. Para la eliminación física de archivos, será necesario ejecutar el comando VACCUM .

# COMMAND ----------

# MAGIC %md
# MAGIC Para confirmar que solo se leen 10 archivos en una consulta, ejecutemos la siguiente consulta y luego verifiquemos el plan de consulta.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Year_Month_Day, count(*) 
# MAGIC FROM nyctaxi 
# MAGIC GROUP BY Year_Month_Day

# COMMAND ----------

# MAGIC %md
# MAGIC Dado que no se aplican condiciones  `where` a esta consulta, el plan de consulta SQL indica que se leyeron 10 archivos, como se esperaba.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ahora podemos agregar una cláusula where en la columna que se optimizó Z-ORDER.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Year_Month_Day, count(*) 
# MAGIC FROM nyctaxi 
# MAGIC WHERE Year_Month_Day = '20191219' 
# MAGIC GROUP BY Year_Month_Day

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. RESUMEN

# COMMAND ----------

# MAGIC %md
# MAGIC - Es importante tener en cuenta que `Z-Ordering` se puede aplicar a varias columnas, sin embargo, se recomienda tener cuidado con este enfoque, ya que agregar demasiadas columnas de Z-Order generará un costo. 
# MAGIC - También hay una función de OPTIMIZACIÓN AUTOMÁTICA que se puede aplicar; sin embargo, la función OPTIMIZACIÓN AUTOMÁTICA no aplicará el orden Z ya que deberá hacerse manualmente. 
# MAGIC - Dada la cantidad significativa de tiempo que lleva ejecutar el comando Z-Order la primera vez, se recomienda considerar ejecutar `Z-ordering` con moderación y como parte de una estrategia de mantenimiento (es decir, semanalmente, etc.). 
# MAGIC - Además, es importante tener en cuenta que Optimize y `Z-Ordering` se pueden ejecutar durante el horario comercial normal y no es necesario que se ejecuten solo como una tarea fuera de línea.
# MAGIC - El orden Z se puede aplicar de forma incremental a particiones y consultas después de la ejecución inicial,

# COMMAND ----------


