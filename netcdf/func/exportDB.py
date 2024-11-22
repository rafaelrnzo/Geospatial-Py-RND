import psycopg2
import csv
from datetime import datetime

def convert_csv_to_postgres(csv_file, db_config, table_name):
    """
    Converts data from a CSV file into PostgreSQL.

    Parameters:
    - csv_file: Path to the CSV file.
    - db_config: Dictionary containing PostgreSQL connection parameters.
    - table_name: PostgreSQL table name.
    """
    conn = None
    cursor = None
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            time TIMESTAMP,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            temperature DOUBLE PRECISION
        );
        """)

        # Read data from the CSV file
        with open(csv_file, mode='r') as file:
            reader = csv.DictReader(file)
            records = []
            for row in reader:
                try:
                    # Parse the date string into a datetime object
                    time = datetime.strptime(row['time'], '%Y-%m-%d')
                    latitude = float(row['latitude'])
                    longitude = float(row['longitude'])
                    # Handle missing temperature values
                    temperature = float(row['temperature']) if row['temperature'] else None

                    records.append((time, latitude, longitude, temperature))
                except Exception as e:
                    print(f"Skipping row due to error: {e}")

        # Batch insert records into PostgreSQL
        if records:
            query = f"""
            INSERT INTO {table_name} (time, latitude, longitude, temperature)
            VALUES (%s, %s, %s, %s)
            """
            cursor.executemany(query, records)
            conn.commit()
            print(f"Inserted {len(records)} records into PostgreSQL table '{table_name}'.")
        else:
            print("No records to insert.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

# PostgreSQL configuration
db_config = {
    'host': 'localhost',
    'port': '5432',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'Falah0918'
}
csv_file_path = r'C:\Users\SD-LORENZO-PC\pyproject\rndPy\Geospatial\netcdf\data\data.csv'
table_name = 'temperature_data'

convert_csv_to_postgres(csv_file_path, db_config, table_name)
