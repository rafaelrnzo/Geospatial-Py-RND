from pyspark.sql import SparkSession
from netCDF4 import Dataset
import numpy as np
import matplotlib.pyplot as plt
from scipy.interpolate import griddata
import cartopy.crs as ccrs
import cartopy.feature as cfeature

# Initialize Spark session
spark = (
    SparkSession.builder.appName("NetCDF to Map")
    .config("spark.driver.memory", "4g")  # Adjust memory as needed
    .getOrCreate()
)

# Path to NetCDF file
nc_file = r'C:\Users\SD-LORENZO-PC\pyproject\rndPy\Geospatial\netcdf\data\1964.nc'

# Read the NetCDF file
data = Dataset(nc_file, mode='r')

# Extract latitude, longitude, and temperature data
lats = data.variables['lat'][:]
lons = data.variables['lon'][:]
tave = data.variables['tave'][:]  # Assuming 'tave' contains temperature data

# Select the first day's temperature data (2D grid)
day_data = tave[0, :, :]

# Flatten the data into a tabular format for Spark
flat_lats, flat_lons = np.meshgrid(lats, lons, indexing='ij')
flat_lats = flat_lats.flatten().tolist()  # Convert to native Python list
flat_lons = flat_lons.flatten().tolist()  # Convert to native Python list
flat_temps = day_data.flatten().tolist()  # Convert to native Python list

# Combine into a list of rows with native Python types
rows = list(zip(flat_lats, flat_lons, flat_temps))

# Define schema explicitly to avoid inference issues
from pyspark.sql.types import StructType, StructField, DoubleType

schema = StructType([
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("temperature", DoubleType(), True)
])

# Create Spark DataFrame
spark_df = spark.createDataFrame(rows, schema=schema)

# Drop rows with invalid temperature values
spark_df = spark_df.filter(spark_df.temperature.isNotNull())

# Convert Spark DataFrame to Pandas DataFrame for visualization
pandas_df = spark_df.toPandas()

# Grid data for heatmap
lat_grid = np.linspace(pandas_df["latitude"].min(), pandas_df["latitude"].max(), 100)
lon_grid = np.linspace(pandas_df["longitude"].min(), pandas_df["longitude"].max(), 100)
lon_mesh, lat_mesh = np.meshgrid(lon_grid, lat_grid)

# Interpolate temperature data onto the grid
temp_grid = griddata(
    (pandas_df["longitude"], pandas_df["latitude"]),
    pandas_df["temperature"],
    (lon_mesh, lat_mesh),
    method="linear"
)

# Plot the map with Cartopy
plt.figure(figsize=(12, 8))
ax = plt.axes(projection=ccrs.PlateCarree())
ax.set_extent([lons.min(), lons.max(), lats.min(), lats.max()], crs=ccrs.PlateCarree())
ax.add_feature(cfeature.COASTLINE)
ax.add_feature(cfeature.BORDERS, linestyle=":")

# Create heatmap
mesh = ax.pcolormesh(lon_mesh, lat_mesh, temp_grid, cmap="jet", shading="auto", transform=ccrs.PlateCarree())
plt.colorbar(mesh, label="Temperature (°C)")

plt.title("Temperature Heatmap from NetCDF Data")
plt.show()

# Stop Spark session
spark.stop()
