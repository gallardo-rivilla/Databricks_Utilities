# Databricks notebook source
# MAGIC %md
# MAGIC # Evolución de Esquema automático en Delta Lake 🎯

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lake no le permite agregar datos con un esquema no coincidente de forma predeterminada

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row

# COMMAND ----------

# Creamos nuestro Spark Dataframe de ejemplo:

usuarios = [
    {
        "id": 1,
        "nombre": "Juan",
        "apellido": "Garcia",
        "email": "juangarcia@gmail.com",
        "direccion": "Calle Los Cristos",
        "edad": 30
    },
    {
        "id": 2,
          "nombre": "Cristina",
        "apellido": "Ruiz",
        "email": "cristinaruiz@gmail.com",
        "direccion": "Calle Federico",
        "edad": 35
    },
    {
        "id": 3,
         "nombre": "Mario",
        "apellido": "Lopez",
        "email": "mariolopez@gmail.com",
        "direccion": "Calle Martirio",
        "edad": 40
    },
    {
        "id": 4,
         "nombre": "Eva",
        "apellido": "Moreno",
        "email": "evamoreno@gmail.com",
        "direccion": "Calle Divina Comedia",
        "edad": 45
    },
    {
        "id": 5,
         "nombre": "David",
        "apellido": "Sanchez",
        "email": "davidsanchez@gmail.com",
        "direccion": "Calle El Faro",
        "edad": 50
    }]
df_usuarios = spark.createDataFrame([Row(**user) for user in usuarios])

# COMMAND ----------

df_usuarios.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Guardamos Delta Table 🔏

# COMMAND ----------

df_usuarios.write.format("delta").save("/tmp/usuarios")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cambiamos el esquema y guardamos de nuevo 🕵️

# COMMAND ----------

# MAGIC %md
# MAGIC Añadimos un nuevo usuario con un esquema diferente, ahora tenemos 2 columnas nuevas: profesion y antiguedad

# COMMAND ----------

nuevo_usuario = [
    {
        "id": 1,
        "nombre": "Alfonso",
        "apellido": "Roldan",
        "email": "alfonsoroldan@gmail.com",
        "direccion": "Calle Córdoba",
        "edad": 35,
        "profesion" : "CEO",
        "antiguedad" : 3
    }
]

df_nuevo_usuario = spark.createDataFrame([Row(**user) for user in nuevo_usuario])

# COMMAND ----------

df_nuevo_usuario.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Guardamos nuevo esquema en tabla Delta  🔏

# COMMAND ----------

# MAGIC %md
# MAGIC Al intentar guardar nos mostrará un mensaje de error **AnalysisException**

# COMMAND ----------

df_nuevo_usuario.write.format("delta").mode("append").save("/tmp/usuarios")

# COMMAND ----------

# MAGIC %md
# MAGIC Veamos una forma de eludir la aplicación del esquema y aprovechar la flexibilidad de la evolución del esquema.

# COMMAND ----------

# MAGIC %md
# MAGIC # 🥇 Usando mergeSchema 🪄

# COMMAND ----------

# MAGIC %md
# MAGIC Puedes establecer la `option("mergeSchema", "true")` para escribir en una tabla Delta y permitir que se agreguen datos con un esquema no coincidente

# COMMAND ----------

df_nuevo_usuario.write.option("mergeSchema", "true").mode("append").format("delta").save("/tmp/usuarios")

# COMMAND ----------

# MAGIC %md
# MAGIC Consultamos los datos después de realizar el cambio de esquema:

# COMMAND ----------

spark.read.format("delta").load("/tmp/usuarios").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **La tabla Delta ahora tiene 8 columnas. Anteriormente solo tenía 6 columnas.**

# COMMAND ----------

# MAGIC %md
# MAGIC Veamos ahora cómo habilitar la evolución del esquema de forma predeterminada.

# COMMAND ----------

# MAGIC %md
# MAGIC # 🥈 Usando autoMerge 🪄

# COMMAND ----------

# MAGIC %md
# MAGIC - Puede habilitar la evolución del esquema de forma predeterminada con `autoMerge` a true
# MAGIC - Permite agregar DataFrames con diferentes esquemas sin configurar mergeSchema.

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cambiamos el esquema y guardamos de nuevo 🕵️

# COMMAND ----------

nuevo_usuario = [
    {
        "id": 1,
        "nombre": "Teresa",
        "apellido": "Virreina",
        "email": "teresavirreina@gmail.com",
        "direccion": "Calle San Cristo",
        "edad": 33,
        "profesion" : "CDO",
        "antiguedad" : 2,
        "salario" : 1.300
    }
]

df_nuevo_usuario = spark.createDataFrame([Row(**user) for user in nuevo_usuario])

# COMMAND ----------

df_nuevo_usuario.write.format("delta").mode("append").save("/tmp/usuarios")


# COMMAND ----------

# MAGIC %md
# MAGIC Consultamos los datos después de realizar el cambio de esquema:

# COMMAND ----------

spark.read.format("delta").load("/tmp/usuarios").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 🚨 Importante 🚨

# COMMAND ----------

# MAGIC %md
# MAGIC - autoMerge te permite evitar la configuración explícita mergeSchema cada vez que agrega datos.
# MAGIC 
# MAGIC - La evolución del esquema también le permite agregar marcos de datos con menos columnas que la tabla Delta existente
# MAGIC 
# MAGIC - Delta Lake mergeSchemasolo aplica para una sola escritura en una sola tabla. Es una buena opción si solo desea habilitar la evolución del esquema para una sola tabla.
# MAGIC 
# MAGIC - La opción de Delta Lake autoMergeactiva la evolución del esquema para escrituras en cualquier tabla. Esto puede ser muy conveniente pero también peligroso.
