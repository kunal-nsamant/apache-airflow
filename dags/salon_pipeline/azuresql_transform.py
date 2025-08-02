import pyodbc
import os
import logging

def run_azuresql_merge(azure_sql_server, azure_sql_database, azure_sql_user, azure_sql_password, sql_file_path):
    """Connects to Azure SQL DB and executes SQL from a file."""

    logging.info("Attempting to connect and run SQL merge script...")

    # Build the connection string
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={azure_sql_server};"
        f"DATABASE={azure_sql_database};"
        f"UID={azure_sql_user};"
        f"PWD={azure_sql_password};"
        f"Encrypt=yes;"      
        f"TrustServerCertificate=no;"
        f"Connection Timeout=30;"
    )

    try:
        # Read the SQL from the file
        with open(sql_file_path, 'r') as file:
            sql_script = file.read()

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Execute the SQL script (pyodbc can execute multiple statements)
        for statement in sql_script.split(';'):
            if statement.strip():
                cursor.execute(statement)

        conn.commit()
        logging.info("SQL merge script executed and committed successfully!")

    except pyodbc.Error as ex:
        logging.error(f"Error executing SQL merge script: {ex}")
        conn.rollback()
        raise # Re-raise to make the Airflow task fail
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()