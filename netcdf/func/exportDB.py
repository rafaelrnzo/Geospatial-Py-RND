import numpy as np
from netCDF4 import Dataset
from pymongo import MongoClient

def convert_netcdf_to_mongo(netcdf_file, mongo_uri, db_name, collection_name):
    """
    Converts data from a NetCDF file into MongoDB.

    Parameters:
    - netcdf_file: Path to the NetCDF file.
    - mongo_uri: MongoDB connection URI.
    - db_name: MongoDB database name.
    - collection_name: MongoDB collection name.
    """
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        data = Dataset(netcdf_file, mode='r')
        lats = data.variables['lat'][:].tolist()  
        lons = data.variables['lon'][:].tolist()
        time = data.variables['time'][:].tolist()
        tave = data.variables['tave'][:]  

        records = []
        for t_idx, t_value in enumerate(time):
            for lat_idx, lat in enumerate(lats):
                for lon_idx, lon in enumerate(lons):
                    record = {
                        "time": t_value,
                        "latitude": lat,
                        "longitude": lon,
                        "temperature": tave[t_idx, lat_idx, lon_idx].item()  # Convert to native type
                    }
                    records.append(record)

        if records:
            collection.insert_many(records)
            print(f"Inserted {len(records)} records into MongoDB.")
        else:
            print("No records to insert.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        client.close()

netcdf_file_path = r'C:\Users\SD-LORENZO-PC\pyproject\rndPy\Geospatial\netcdf\data\1964.nc'
mongo_uri = "mongodb+srv://devadmin:2553669874123654@dev218.kke0v.mongodb.net/?retryWrites=true&w=majority&appName=dev218"
database_name = "climate_data"
collection_name = "temperature_data"

convert_netcdf_to_mongo(netcdf_file_path, mongo_uri, database_name, collection_name)
