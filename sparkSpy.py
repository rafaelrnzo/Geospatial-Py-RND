import findspark
findspark.init()

from pyspark.sql import SparkSession
from datetime import datetime, date
from pyspark.sql import Row
import pymongo
from pymongo import MongoClient
import numpy as np
import matplotlib.pyplot as plt
from netCDF4 import Dataset
import cartopy.crs as ccrs
import cartopy.feature as cfeature

# MongoDB connection details
mongo_uri = "mongodb+srv://devadmin:2553669874123654@dev218.kke0v.mongodb.net/?retryWrites=true&w=majority&appName=dev218"
database_name = "climate_data"
collection_name = "temperature_data"

# Initialize MongoDB connection
client = MongoClient(mongo_uri)
db = client[database_name]
collection = db[collection_name]

# Fetch data from MongoDB
data_from_mongo = list(collection.find({}, {"_id": 0}))  # Fetch all documents without '_id'

# Print the fetched data
print("Data from MongoDB:", data_from_mongo)

# Initialize PySpark
spark = SparkSession.builder.getOrCreate()

# Convert MongoDB data to PySpark DataFrame
df = spark.createDataFrame(data_from_mongo)

# Show the PySpark DataFrame
df.show()

# Example: Process MongoDB data if needed
processed_data = df.collect()
lats = [row['lat'] for row in processed_data]
lons = [row['lon'] for row in processed_data]
tave = [row['temperature'] for row in processed_data]

# Plot with Matplotlib and Cartopy
plt.figure(figsize=(12, 8))
ax = plt.axes(projection=ccrs.PlateCarree())
ax.set_extent([min(lons), max(lons), min(lats), max(lats)], crs=ccrs.PlateCarree())
ax.add_feature(cfeature.COASTLINE)
ax.add_feature(cfeature.BORDERS, linestyle=':')

# Create heatmap
mesh = plt.scatter(lons, lats, c=tave, cmap='jet', transform=ccrs.PlateCarree())
plt.colorbar(mesh, label="Temperature (°C)")

plt.title('Temperature Data from MongoDB')
plt.show()
