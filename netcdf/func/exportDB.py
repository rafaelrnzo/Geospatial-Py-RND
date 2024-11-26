import pandas as pd
from pymongo import MongoClient

def convert_csv_to_mongo(csv_file, mongo_uri, db_name, collection_name):
    """
    Converts data from a CSV file into MongoDB.

    Parameters:
    - csv_file: Path to the CSV file.
    - mongo_uri: MongoDB connection URI.
    - db_name: MongoDB database name.
    - collection_name: MongoDB collection name.
    """
    try:
        df = pd.read_csv(csv_file)
        records = df.to_dict(orient='records')

        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        if records:
            collection.insert_many(records)
            print(f"Inserted {len(records)} records into MongoDB.")
        else:
            print("No records to insert.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        client.close()

# Parameters
csv_file_path = r'C:\Users\SD-LORENZO-PC\pyproject\rndPy\Geospatial\netcdf\data\data.csv'
mongo_uri = "mongodb+srv://devadmin:2553669874123654@dev218.kke0v.mongodb.net/?retryWrites=true&w=majority&appName=dev218"
database_name = "climate_data"
collection_name = "temperature_data"

convert_csv_to_mongo(csv_file_path, mongo_uri, database_name, collection_name)
