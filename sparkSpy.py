import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import numpy as np
from pyspark.sql import SparkSession
import os 
import sys

# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName("ClimateDataVisualization") \
    .config("spark.mongodb.read.connection.uri", "mongodb+srv://devadmin:2553669874123654@dev218.kke0v.mongodb.net/climate_data.temperature_data?retryWrites=true&w=majority") \
    .getOrCreate()

mongo_uri = "mongodb+srv://devadmin:2553669874123654@dev218.kke0v.mongodb.net/climate_data.temperature_data?retryWrites=true&w=majority"
climate_data_df = spark.read.format("mongodb").load()

df = climate_data_df.select("lat", "lon", "anom").toPandas()

print("Data fetched from MongoDB using Spark:")
print(df.head())

lats = df['lat'].values
lons = df['lon'].values
anoms = df['anom'].values

lon_grid, lat_grid = np.meshgrid(np.unique(lons), np.unique(lats))

anom_grid = np.reshape(anoms, lon_grid.shape)

plt.figure(figsize=(10, 5))

ax = plt.axes(projection=ccrs.PlateCarree())

ax.coastlines()
ax.set_xticks(range(-180, 181, 60), crs=ccrs.PlateCarree())
ax.set_yticks(range(-90, 90, 30), crs=ccrs.PlateCarree())
ax.set_xlabel('Longitude')
ax.set_ylabel('Latitude')


c = ax.contourf(lon_grid, lat_grid, anom_grid, levels=20, cmap="seismic", transform=ccrs.PlateCarree())

plt.colorbar(c, label='Temperature Anomaly (°C)')

plt.title('Temperature Anomalies from MongoDB via Apache Spark')

plt.show()

spark.stop()
