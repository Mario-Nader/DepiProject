import pandas as pd
import pyodbc
import os
import time  # Import the time module
from dotenv import load_dotenv

# --- CONFIGURATION (Stays the same) ---
load_dotenv() 

# Azure SQL Database connection details (REPLACE WITH YOURS)
server = 'traffic-project22.database.windows.net' # <-- REPLACE
database = 'traffic' # <-- REPLACE
username = 'kero' # <-- REPLACE
password = 'Kk123456' # Gets password from .env file
driver = '{ODBC Driver 18 for SQL Server}' # May need to change based on your install


# CSV and Table details
csv_file_path = 'traffic_data.csv'
table_name = 'TrafficLog'
rows_to_upload = 4
run_interval_seconds = 10 # How many seconds to wait between runs

# --- MAIN UPLOAD FUNCTION ---
# We put the core logic in a function to make the loop cleaner
def upload_random_traffic_data():
    """
    Reads the CSV, selects random rows, and uploads them to Azure SQL.
    """
    try:
        # 1. Read CSV and select random rows
        print(f"Reading data from {csv_file_path}...")
        df = pd.read_csv(csv_file_path)
        random_rows = df.sample(n=rows_to_upload)
        print(f"Selected {len(random_rows)} random rows to upload:")
        print(random_rows.head())

    except FileNotFoundError:
        print(f"Error: The file {csv_file_path} was not found. Halting this run.")
        return # Stop this function execution if file not found
    except Exception as e:
        print(f"An error occurred while reading the CSV: {e}")
        return

    # 2. Connect to the database and upload data
    connection_string = f'DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
    
    try:
        print("\nConnecting to Azure SQL Database...")
        with pyodbc.connect(connection_string, timeout=30) as cnxn:
            cursor = cnxn.cursor()
            print("Connection successful.")

            # Note: The table creation step will only run once if the table doesn't exist.
            # On subsequent runs, it will see the table is there and do nothing.
            create_table_sql = f"""
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{table_name}]') AND type in (N'U'))
            BEGIN
            CREATE TABLE [dbo].[{table_name}](
                [Time] [time] NULL, [Date] [int] NULL, [DayOfTheWeek] [nvarchar](20) NULL,
                [CarCount] [int] NULL, [BikeCount] [int] NULL, [BusCount] [int] NULL,
                [TruckCount] [int] NULL, [Total] [int] NULL, [TrafficSituation] [nvarchar](50) NULL
            )
            END
            """
            cursor.execute(create_table_sql)
            cnxn.commit()

            # 4. Insert the random rows into the table
            print(f"Inserting {len(random_rows)} rows...")
            insert_sql = "INSERT INTO {} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);".format(table_name)
            
            for index, row in random_rows.iterrows():
                cursor.execute(insert_sql, tuple(row))
            
            cnxn.commit()
            print("Data uploaded successfully!")

    except pyodbc.Error as ex:
        print(f"A database error occurred: {ex}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# --- INFINITE LOOP ---
# This is the main part of the script that runs continuously.
if __name__ == "__main__":
    run_count = 0
    print("--- Starting continuous data upload script ---")
    print(f"--- A new batch will be uploaded every {run_interval_seconds} seconds ---")
    print("--- Press Ctrl+C to stop the script ---")
    
    while True:
        try:
            run_count += 1
            print("\n" + "="*50)
            print(f"Starting Run #{run_count}")
            print("="*50)
            
            # Call the main function to do the work
            upload_random_traffic_data()
            
            print(f"\n--- Run #{run_count} complete. Waiting for {run_interval_seconds} seconds... ---")
            time.sleep(run_interval_seconds) # Wait for 10 seconds

        except KeyboardInterrupt:
            # This allows you to stop the script gracefully
            print("\nScript stopped by user. Exiting.")
            break
        except Exception as e:
            # Catch any other unexpected errors to prevent the loop from crashing
            print(f"A critical error occurred: {e}. The script will try again after the interval.")
            time.sleep(run_interval_seconds)
            