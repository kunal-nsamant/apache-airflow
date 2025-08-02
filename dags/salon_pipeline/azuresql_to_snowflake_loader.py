import pyodbc
import snowflake.connector
import pandas as pd
import numpy as np
import datetime

# --- Refactored Function for Airflow ---
# IMPORTANT: For production, Airflow Connections or environment variables are preferred for security.
# For local testing, placing credentials directly within the function is acceptable.
def run_azuresql_to_snowflake_load(): 
    # --- Configuration ---
    # Azure SQL Database Details (Your salon_stage_db)
    AZURE_SQL_SERVER = "mysalonpipeline-server-112131.database.windows.net" 
    AZURE_SQL_DATABASE = "salon_stage_db"
    AZURE_SQL_USER = "sqladmin" 
    AZURE_SQL_PASSWORD = "Bmarkham555g@salonstage" 

    # Snowflake Details (from your signup)
    SNOWFLAKE_ACCOUNT = "FSILXZI-PQ06615" 
    SNOWFLAKE_USER = "KSAMANTHAWK" 
    SNOWFLAKE_PASSWORD = "Bmarkham555@AWSsnowflake" # <--- IMPORTANT: YOU NEED TO PUT YOUR SNOWFLAKE PASSWORD HERE
    SNOWFLAKE_WAREHOUSE = "COMPUTE_WH" 
    SNOWFLAKE_DATABASE = "SALON_ANALYTICS"
    SNOWFLAKE_SCHEMA = "RAW_DATA"

    # --- Function to Extract Data from Azure SQL DB ---
    def extract_from_azuresql(table_name):
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={AZURE_SQL_SERVER};"
            f"DATABASE={AZURE_SQL_DATABASE};"
            f"UID={AZURE_SQL_USER};"
            f"PWD={AZURE_SQL_PASSWORD};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
        try:
            conn = pyodbc.connect(conn_str)
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, conn)
            print(f"Extracted {len(df)} records from Azure SQL DB table: {table_name}")
            return df
        except pyodbc.Error as ex:
            print(f"Error extracting from Azure SQL DB: {ex}")
            raise # Re-raise error to make Airflow task fail
        finally:
            if 'conn' in locals():
                conn.close()

    # --- Helper to map pandas dtypes to Snowflake SQL types ---
    def map_dtype_to_snowflake_sql_type(dtype_name):
        if 'int' in dtype_name:
            return 'INT'
        elif 'float' in dtype_name:
            return 'FLOAT'
        elif 'bool' in dtype_name:
            return 'BOOLEAN'
        elif 'datetime' in dtype_name:
            return 'TIMESTAMP_NTZ'
        elif 'object' in dtype_name: # Pandas 'object' dtype usually means string
            return 'VARCHAR(255)' # Adjust size if needed, or make it dynamic
        else:
            return 'VARIANT' # Fallback for unhandled types

    # --- Function to Load Data to Snowflake (using MERGE for UPSERT/Soft Delete) ---
    def load_to_snowflake(df, snowflake_table_name, primary_key_column):
        if df.empty:
            print(f"No data to load into Snowflake table: {snowflake_table_name}. Skipping.")
            return

        try:
            conn = snowflake.connector.connect(
                user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PASSWORD,
                account=SNOWFLAKE_ACCOUNT,
                warehouse=SNOWFLAKE_WAREHOUSE,
                database=SNOWFLAKE_DATABASE,
                schema=SNOWFLAKE_SCHEMA
            )
            cursor = conn.cursor()

            # For Snowflake, we'll first load into a temporary staging table,
            # then MERGE from the temporary table to the final table.
            temp_table_name = f"TEMP_{snowflake_table_name}"

            # Prepare columns for copy into (all columns from DataFrame)
            column_definitions = ", ".join([
                f'"{col.upper()}" {map_dtype_to_snowflake_sql_type(df[col].dtype.name)}'
                for col in df.columns
            ])
            columns_for_insert = ", ".join([f'"{col.upper()}"' for col in df.columns])


            # Create a temporary table with the same structure as the DataFrame
            create_temp_table_sql = f"""
            CREATE OR REPLACE TEMPORARY TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{temp_table_name} (
                {column_definitions}
            );
            """
            cursor.execute(create_temp_table_sql)
            print(f"Created temporary Snowflake table: {temp_table_name}")

            # Prepare data for insertion (Snowflake copy_into supports list of tuples)
            data_to_insert = []
            for _, row in df.iterrows():
                processed_row = []
                for item in row:
                    if pd.isna(item): # Handles both numpy.nan and pandas.NaT
                        processed_row.append(None)
                    elif isinstance(item, (datetime.datetime, datetime.date)):
                        processed_row.append(item.strftime('%Y-%m-%d %H:%M:%S.%f')) # Format datetime to string
                    else:
                        processed_row.append(item)
                data_to_insert.append(tuple(processed_row))


            # Use executemany for efficient insertion into temporary table
            insert_sql = f"INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{temp_table_name} ({columns_for_insert}) VALUES ({', '.join(['%s'] * len(df.columns))})"
            cursor.executemany(insert_sql, data_to_insert)
            
            print(f"Loaded {len(df)} records into temporary Snowflake table: {temp_table_name}")

            # MERGE statement for UPSERT/Soft Delete
            if snowflake_table_name.upper() == 'CLIENTS':
                merge_sql = f"""
                MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{snowflake_table_name} AS target
                USING {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{temp_table_name} AS source
                ON target.{primary_key_column.upper()} = source.{primary_key_column.upper()}
                WHEN MATCHED THEN
                    UPDATE SET
                        target.NAME = source.NAME,
                        target.EMAIL = source.EMAIL,
                        target.PHONE = source.PHONE,
                        target.CREATED_AT = source.CREATED_AT,
                        target.UPDATED_AT = source.UPDATED_AT,
                        target.DELETED_AT = source.DELETED_AT
                WHEN NOT MATCHED THEN
                    INSERT ({columns_for_insert})
                    VALUES ({columns_for_insert});
                """
            elif snowflake_table_name.upper() == 'APPOINTMENTS':
                merge_sql = f"""
                MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{snowflake_table_name} AS target
                USING {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{temp_table_name} AS source
                ON target.{primary_key_column.upper()} = source.{primary_key_column.upper()}
                WHEN MATCHED THEN
                    UPDATE SET
                        target.CLIENT_ID = source.CLIENT_ID,
                        target.SERVICE = source.SERVICE,
                        target.APPOINTMENT_TIME = source.APPOINTMENT_TIME,
                        target.STATUS = source.STATUS,
                        target.CREATED_AT = source.CREATED_AT,
                        target.UPDATED_AT = source.UPDATED_AT,
                        target.DELETED_AT = source.DELETED_AT
                WHEN NOT MATCHED THEN
                    INSERT ({columns_for_insert})
                    VALUES ({columns_for_insert});
                """
            else:
                raise ValueError(f"Unknown table for MERGE logic: {snowflake_table_name}")

            cursor.execute(merge_sql)
            print(f"Successfully merged data into Snowflake table: {snowflake_table_name}")

        except snowflake.connector.Error as e:
            print(f"Error connecting to Snowflake or loading data: {e}")
            raise # Re-raise error to make Airflow task fail
        finally:
            if 'conn' in locals():
                cursor.close()
                conn.close()

# This block is for testing the function directly if needed, not used by Airflow
if __name__ == "__main__":
    run_azuresql_to_snowflake_load()