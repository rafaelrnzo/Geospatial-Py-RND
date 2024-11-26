import matplotlib.pyplot as plt
import cartopy.crs as ccrs
from pyspark.sql import SparkSession
import pandas as pd

# Initialize Spark session with MongoDB connection
spark = SparkSession.builder \
    .appName("ShipwrecksVisualization") \
    .config("spark.mongodb.read.connection.uri", "mongodb+srv://devadmin:2553669874123654@dev218.kke0v.mongodb.net/sample_geospatial.shipwrecks?retryWrites=true&w=majority") \
    .getOrCreate()

# Load data from MongoDB
shipwrecks_df = spark.read.format("mongodb").load()

# Select relevant fields and calculate the average depth by feature type
average_depth_df = shipwrecks_df.select("feature_type", "depth") \
    .groupby("feature_type") \
    .avg("depth") \
    .withColumnRenamed("avg(depth)", "average_depth")  # Rename column for clarity

# Convert the Spark DataFrame to Pandas for visualization
average_depth_pd = average_depth_df.toPandas()

# Display the average depth table
print("Average Depth by Feature Type:")
print(average_depth_pd)

# Prepare data for visualization
shipwrecks_pd = shipwrecks_df.select("latdec", "londec", "feature_type").toPandas()
latitudes = shipwrecks_pd['latdec'].astype(float)
longitudes = shipwrecks_pd['londec'].astype(float)

# Plot shipwreck locations on a map
plt.figure(figsize=(12, 6))
ax = plt.axes(projection=ccrs.PlateCarree())
ax.coastlines()
ax.set_global()
ax.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False)

# Scatter plot for shipwreck locations
plt.scatter(longitudes, latitudes, c='red', s=10, label="Shipwrecks", transform=ccrs.PlateCarree())
plt.legend()

plt.title('Shipwrecks Locations (Geospatial Data)')
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.show()

# Bar plot for average depth by feature type
plt.figure(figsize=(10, 6))
plt.barh(average_depth_pd["feature_type"], average_depth_pd["average_depth"], color="blue")
plt.title("Average Depth by Shipwreck Feature Type")
plt.xlabel("Average Depth")
plt.ylabel("Feature Type")
plt.tight_layout()
plt.show()

# Stop Spark session
spark.stop()
