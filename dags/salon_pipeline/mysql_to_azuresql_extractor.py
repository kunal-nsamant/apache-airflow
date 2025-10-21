import mysql.connector
import pandas as pd
import pyodbc
import datetime

# --- Refactored Function for Airflow ---
# IMPORTANT: For production, Airflow Connections or environment variables are preferred for security.
# For local testing, placing credentials directly within the function is acceptable.
def run_mysql_extraction(): 
    # --- Configuration ---
    # MySQL Details (Your Local MySQL)
    MYSQL_HOST = "host.docker.internal" 
    MYSQL_PORT = 3306
    MYSQL_USER = "root"
    MYSQL_PASSWORD = "11213141" 
    MYSQL_DATABASE = "salon_db"

    # Azure SQL Database Details (Your salon_stage_db)
    AZURE_SQL_SERVER = "mysalonpipeline-server-112131.database.database.windows.net" 
    AZURE_SQL_DATABASE = "salon_stage_db"
    AZURE_SQL_USER = "sqladmin" 
    AZURE_SQL_PASSWORD = "Bmarkham555@salonstage" 

    # CDC Watermark (read from control table in Azure SQL DB)
    last_extraction_timestamp_clients = "1900-01-01 00:00:00" # Initial dummy value if cannot retrieve
    last_extraction_timestamp_appointments = "1900-01-01 00:00:00" # Initial dummy value if cannot retrieve

    # --- Get last processed timestamp from Azure SQL DB control table ---
    conn_str_azure = (
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
        azure_conn = pyodbc.connect(conn_str_azure)
        azure_cursor = azure_conn.cursor()
        
        azure_cursor.execute("SELECT last_processed_timestamp FROM cdc_watermarks WHERE table_name = 'clients'")
        result = azure_cursor.fetchone()
        if result and result[0] is not None:
            # CORRECTED LINE HERE: Removed the extra space in '%m- %d'
            last_extraction_timestamp_clients = result[0].strftime('%Y-%m-%d %H:%M:%S')

        azure_cursor.execute("SELECT last_processed_timestamp FROM cdc_watermarks WHERE table_name = 'appointments'")
        result = azure_cursor.fetchone()
        if result and result[0] is not None:
            # CORRECTED LINE HERE: Removed the extra space in '%m- %d'
            last_extraction_timestamp_appointments = result[0].strftime('%Y-%m-%d %H:%M:%S')

        print(f"Loaded watermarks: clients={last_extraction_timestamp_clients}, appointments={last_extraction_timestamp_appointments}")

    except pyodbc.Error as ex:
        print(f"Error reading watermark from Azure SQL DB: {ex}")
        # Decide how to handle this error: fail pipeline, use default timestamp, etc.
    finally:
        if 'azure_conn' in locals() and azure_conn:
            azure_cursor.close()
            azure_conn.close()


    # --- Function to Extract Data from MySQL ---
    def extract_from_mysql(table_name, last_ts):
        try:
            conn = mysql.connector.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DATABASE
            )
            cursor = conn.cursor(dictionary=True) # Returns rows as dictionaries

            query = f"""
            SELECT *,
                CASE
                    WHEN deleted_at IS NOT NULL AND deleted_at > '{last_ts}' THEN 'DELETE'
                    WHEN updated_at > '{last_ts}' AND deleted_at IS NULL THEN 'UPSERT'
                    ELSE 'NO_CHANGE'
                END as _operation_type
            FROM {table_name}
            WHERE updated_at > '{last_ts}' OR (deleted_at IS NOT NULL AND deleted_at > '{last_ts}');
            """
            
            print(f"Executing MySQL query for {table_name}: {query}")
            cursor.execute(query)
            records = cursor.fetchall()
            df = pd.DataFrame(records)
            
            if '_operation_type' in df.columns:
                df.rename(columns={'_operation_type': '_operation'}, inplace=True)

            return df

        except mysql.connector.Error as err:
            print(f"Error connecting to MySQL or executing query: {err}")
            raise # Re-raise error to make Airflow task fail
        finally:
            if 'conn' in locals() and conn.is_connected():
                cursor.close()
                conn.close()

    # --- Function to Load Data to Azure SQL Database ---
    def load_to_azure_sql(df, table_name):
        if df.empty:
            print(f"No data to load into {table_name}. Skipping.")
            return

        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={AZURE_SQL_SERVER};"
            f"DATABASE={AZURE_SQL_DATABASE};"
            f"UID={AZURE_SQL_USER};"
            f"PWD={AZURE_SQL_PASSWORD}"
        )

        try:
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()

            cursor.execute(f"TRUNCATE TABLE {table_name}")
            conn.commit()
            print(f"Truncated {table_name} in Azure SQL DB.")

            columns = df.columns.tolist()
            if '_operation' in columns:
                columns.remove('_operation')
                columns.append('_operation')
            
            placeholders = ', '.join(['?'] * len(columns))
            sql_insert = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

            print(f"Loading {len(df)} records into {table_name}...")
            data_to_insert = [tuple(row[col] for col in columns) for index, row in df.iterrows()]
            
            cursor.executemany(sql_insert, data_to_insert)
            conn.commit()
            print(f"Successfully loaded {len(df)} records into {table_name}.")

        except pyodbc.Error as ex:
            print(f"Error connecting to Azure SQL DB or loading data: {ex}")
            conn.rollback()
            raise # Re-raise error to make Airflow task fail
        finally:
            if 'conn' in locals():
                cursor.close()
                conn.close()

    # --- Main Execution Logic (inside the run_mysql_extraction function) ---
    print("Starting data extraction and load process...")

    # Process Clients Table
    print("\n--- Processing Clients Table ---")
    clients_raw_df = extract_from_mysql("clients", last_extraction_timestamp_clients)
    if not clients_raw_df.empty:
        load_to_azure_sql(clients_raw_df, "raw_clients_staging")
    else:
        print("No new/updated clients data to process.")

    # Process Appointments Table
    print("\n--- Processing Appointments Table ---")
    appointments_raw_df = extract_from_mysql("appointments", last_extraction_timestamp_appointments)
    if not appointments_raw_df.empty:
        load_to_azure_sql(appointments_raw_df, "raw_appointments_staging")
    else:
        print("No new/updated appointments data to process.")

    print("\nData extraction and load process finished.")

# This block is for testing the function directly if needed, not used by Airflow
if __name__ == "__main__":
    run_mysql_extraction()