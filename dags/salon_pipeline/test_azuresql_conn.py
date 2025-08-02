import pyodbc

# Azure SQL Database Details - DOUBLE CHECK THESE CAREFULLY
AZURE_SQL_SERVER = "mysalonpipeline-server-112131.database.windows.net"
AZURE_SQL_DATABASE = "salon_stage_db"
AZURE_SQL_USER = "sqladmin"
AZURE_SQL_PASSWORD = "Bmarkham@555@salonstage" # <--- YOUR PASSWORD HERE

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={AZURE_SQL_SERVER};"
    f"DATABASE={AZURE_SQL_DATABASE};"
    f"UID={AZURE_SQL_USER};"
    f"PWD={AZURE_SQL_PASSWORD};"
    f"Encrypt=yes;"      # Ensure encryption is explicitly requested
    f"TrustServerCertificate=no;" # Ensure certificate is validated
    f"Connection Timeout=30;" # Increase timeout to 30 seconds
)

try:
    print(f"Attempting to connect to {AZURE_SQL_SERVER}/{AZURE_SQL_DATABASE}...")
    conn = pyodbc.connect(conn_str)
    print("Connection successful!")
    cursor = conn.cursor()
    cursor.execute("SELECT GETDATE() AS CurrentDateTime;")
    row = cursor.fetchone()
    print(f"Current Date/Time from Azure SQL DB: {row[0]}")
    cursor.close()
    conn.close()
except pyodbc.Error as ex:
    sqlstate = ex.args[0]
    print(f"Connection failed! SQLSTATE: {sqlstate}")
    print(f"Error details: {ex.args[1]}")