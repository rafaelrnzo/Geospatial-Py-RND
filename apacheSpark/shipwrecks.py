import matplotlib.pyplot as plt
import cartopy.crs as ccrs
from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder \
    .appName("ShipwrecksVisualization") \
    .config("spark.mongodb.read.connection.uri", "mongodb+srv://devadmin:2553669874123654@dev218.kke0v.mongodb.net/sample_geospatial.shipwrecks?retryWrites=true&w=majority") \
    .getOrCreate()

mongo_uri = "mongodb+srv://devadmin:2553669874123654@dev218.kke0v.mongodb.net/sample_geospatial.shipwrecks?retryWrites=true&w=majority"
shipwrecks_df = spark.read.format("mongodb").load()

shipwrecks_pd = shipwrecks_df.select("latdec", "londec", "feature_type").toPandas()

print("Sample Data:")
print(shipwrecks_pd.head())

latitudes = shipwrecks_pd['latdec'].astype(float)
longitudes = shipwrecks_pd['londec'].astype(float)
feature_types = shipwrecks_pd['feature_type']

plt.figure(figsize=(12, 6))
ax = plt.axes(projection=ccrs.PlateCarree())

ax.coastlines()
ax.set_global()
ax.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False)

plt.scatter(longitudes, latitudes, c='red', s=10, label="Shipwrecks", transform=ccrs.PlateCarree())
plt.legend()

plt.title('Shipwrecks Locations (Geospatial Data)')
plt.xlabel('Longitude')
plt.ylabel('Latitude')

plt.show()

spark.stop()
