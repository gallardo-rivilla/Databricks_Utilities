# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Migrate Managed Table to Unity Catalog# 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - www.gallardorivilla.es
# MAGIC - https://github.com/gallardo-rivilla

# COMMAND ----------

# MAGIC %md
# MAGIC - Este script tiene como objetivo migrar tablas administradas desde un catálogo de origen en Hive Metastore a un nuevo catálogo en Unity Catalog en Databricks. 
# MAGIC - Lo hace asegurando que los nombres de las columnas sean válidos y que los datos se almacenen en una ubicación externa especificada en formato Delta. 
# MAGIC - La migración se realiza de manera concurrente para mejorar la eficiencia, utilizando el módulo concurrent.futures.

# COMMAND ----------

# DBTITLE 1,1️⃣ Load function
import concurrent.futures
import re


def clean_column_name(col_name):
    """
    Cleans the column name by replacing invalid characters with underscores.
    """
    return re.sub(r'[ ,;{}()\n\t=]', '_', col_name)


def migrate_managed_tables_to_unity_catalog(src_ct_name, src_databases,
                                            exclude_databases, name_location,
                                            external_location):
    """
    Copies all managed tables from one catalog to another catalog.

    Parameters:
        src_ct_name (str): The name of the source catalog.
        src_databases (list): A list of dictionaries representing the 
                              databases in the source catalog.
        exclude_databases (list): A list of database names to exclude 
                                  from migration.
        name_location (str): The name of the target catalog. It must be 
                             created beforehand.
        external_location (str): The file system location where the 
                                 external table data will be stored.

    Returns:
        None

    Example:
        src_ct_name = "hive_metastore"
        src_databases = spark.sql(f"SHOW DATABASES IN {src_ct_name}").collect()
        dst_ct_name = "uc_te"
        exclude_databases = ["default"]
        name_location = "migra"
        external_location = "/mnt/external_tables/"
        migrate_managed_tables_to_unity_catalog(
            src_ct_name, src_databases, dst_ct_name, exclude_databases, 
            external_location
        )
    """
    list_of_db = [db['databaseName'] for db in src_databases]

    if exclude_databases:
        databases = [db for db in list_of_db if db not in exclude_databases]
    else:
        databases = list_of_db

    print(f"Databases to migrate from HiveMetastore: 💫 {databases}")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {name_location}")
    print(f"Catalog created: ✅ {name_location}")

    # Process all tables in the database
    for db in databases:
        query = f"SHOW TABLES IN {src_ct_name}.{db}"
        list_of_tables = spark.sql(query).collect()
        print(f"Total tables in {db}: {len(list_of_tables)}")

        for table in list_of_tables:
            def process_table(db, table):
                try:
                    table_name = table['tableName']
                    q = f"DESCRIBE TABLE EXTENDED {src_ct_name}.{db}.{table_name}"
                    d = spark.sql(q).filter("col_name == 'Type'").collect()

                    if d and d[0]['data_type'] == 'MANAGED':
                        print(f"Migrating table: 👉 {table_name} 🚀 from database: {db}")

                        # Read table data
                        df = spark.table(f"{src_ct_name}.{db}.{table_name}")

                        # Clean column names
                        new_columns = [clean_column_name(col) for col in df.columns]
                        for old_col, new_col in zip(df.columns, new_columns):
                            df = df.withColumnRenamed(old_col, new_col)

                        # Define location for the external table data
                        external_table_path = f"{external_location}/{db}/{table_name}"

                        # Save data to the specified location in Delta format
                        df.write.format("delta").mode('overwrite').save(external_table_path)

                        # Create the parent external location
                        spark.sql(f"CREATE CATALOG IF NOT EXISTS {name_location}")
                        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {name_location}.{db}")

                        # Create the external table in the target catalog
                        spark.sql(f"DROP TABLE IF EXISTS {name_location}.{db}.{table_name}")
                        spark.sql(f"""
                            CREATE TABLE {name_location}.{db}.{table_name} 
                            USING DELTA 
                            LOCATION '{external_table_path}'
                        """)
                    else:
                        print(f"Does not meet criteria: {table_name}")

                except Exception as e:
                    print(f"Error processing table {table_name} in database {db}: {e}")
                    raise

            def process_database(db):
                try:
                    # Create the database in the target catalog if it doesn't exist
                    create_db = f"CREATE DATABASE IF NOT EXISTS {name_location}.{db}"
                    spark.sql(create_db)

                    query = f"SHOW TABLES IN {src_ct_name}.{db}"
                    list_of_tables = spark.sql(query).collect()
                    print(f"Total tables in {db}: {len(list_of_tables)}")

                    # Process all tables in the database
                    for table in list_of_tables:
                        print(f"Processing table: {table['tableName']}")

                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        futures = [executor.submit(process_table, db, tb) for tb in list_of_tables]
                        for future in concurrent.futures.as_completed(futures):
                            try:
                                future.result()
                            except Exception as e:
                                print(f"Error processing table: {e}")

                except Exception as e:
                    print(f"Error processing database {db}: {e}")
                    raise

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_database, db) for db in databases]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error processing database: {e}")


# COMMAND ----------

# DBTITLE 1,2️⃣ List all Hive Metastore tables
src_ct_name = "hive_metastore"
databases = spark.sql(f"SHOW DATABASES IN {src_ct_name}").collect()
database_names = [db['databaseName'] for db in databases]
database_names

# COMMAND ----------

# MAGIC %md
# MAGIC Esta es una función de Python llamada migrar_managed_tables_to_unity_catalog. El propósito de esta función es copiar todas las tablas administradas de un catálogo de origen a un catálogo de destino. La función acepta cuatro parámetros:
# MAGIC
# MAGIC - src_ct_name: el nombre del catálogo de origen.
# MAGIC - src_databases: una lista  de bases de datos del catálogo de origen.
# MAGIC - dst_ct_name: el nombre del catálogo de destino.
# MAGIC - exclude databases: una lista de nombres de bases de datos que se excluirán de la migración.

# COMMAND ----------

# DBTITLE 1,3️⃣ Run Script Migra Hive Metastore
# Source catalog name, default is hive_metastore
src_ct_name = dbutils.widgets.get("src_ct_name")

# List of databases from the source catalog
src_databases = spark.sql(f"SHOW DATABASES IN {src_ct_name}").collect()

# Target catalog name
name_location = dbutils.widgets.get("name_location")

# Storage location where the migrated external tables will be stored
external_location = dbutils.widgets.get("external_location")

# List of databases to exclude from the migration
exclude_databases = dbutils.widgets.get("exclude_databases").split(',')

print(f"📢 - Databases excluded from migration: {exclude_databases}")

# Error handling when calling the migration function
try:
    # Use the function to migrate tables from a metastore to a unity catalog
    migrate_managed_tables_to_unity_catalog(
        src_ct_name, src_databases, exclude_databases, name_location, external_location
    )
    print("✅ - Migration completed successfully.")
except Exception as e:
    print(f"❌ - Error during migration: {e}")

# COMMAND ----------

# DBTITLE 1,4️⃣ Delete Catalog
# MAGIC %sql
# MAGIC -- DROP CATALOG migra CASCADE;
