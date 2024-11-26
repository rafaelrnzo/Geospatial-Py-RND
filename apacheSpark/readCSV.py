from pyspark.sql import SparkSession
from netCDF4 import Dataset
import numpy as np
import matplotlib.pyplot as plt
from scipy.interpolate import griddata
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from pyspark.sql.types import StructType, StructField, DoubleType

spark = (
    SparkSession.builder.appName("NetCDF to Map")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

nc_file = r'C:\Users\SD-LORENZO-PC\pyproject\rndPy\Geospatial\netcdf\data\1964.nc'

data = Dataset(nc_file, mode='r')

lats = data.variables['lat'][:]
lons = data.variables['lon'][:]
tave = data.variables['tave'][:]  # Assuming 'tave' contains temperature data

day_data = tave[0, :, :]

flat_lats, flat_lons = np.meshgrid(lats, lons, indexing='ij')
flat_lats = flat_lats.flatten().tolist()
flat_lons = flat_lons.flatten().tolist()
flat_temps = day_data.flatten().tolist()

rows = list(zip(flat_lats, flat_lons, flat_temps))

schema = StructType([
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("temperature", DoubleType(), True)
])

spark_df = spark.createDataFrame(rows, schema=schema)

spark_df = spark_df.filter(spark_df.temperature.isNotNull())

pandas_df = spark_df.toPandas()

grid_resolution = 200  # Increase resolution for smoother heatmap
lat_grid = np.linspace(pandas_df["latitude"].min(), pandas_df["latitude"].max(), grid_resolution)
lon_grid = np.linspace(pandas_df["longitude"].min(), pandas_df["longitude"].max(), grid_resolution)
lon_mesh, lat_mesh = np.meshgrid(lon_grid, lat_grid)

temp_grid = griddata(
    (pandas_df["longitude"], pandas_df["latitude"]),
    pandas_df["temperature"],
    (lon_mesh, lat_mesh),
    method="cubic"  # Use cubic interpolation for smooth results
)

plt.figure(figsize=(12, 8))
ax = plt.axes(projection=ccrs.PlateCarree())
ax.set_extent([lons.min(), lons.max(), lats.min(), lats.max()], crs=ccrs.PlateCarree())
ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
ax.add_feature(cfeature.BORDERS, linestyle=":", linewidth=0.5)
ax.add_feature(cfeature.LAND, edgecolor='black', alpha=0.3)
ax.add_feature(cfeature.RIVERS, alpha=0.2)

mesh = ax.pcolormesh(
    lon_mesh, lat_mesh, temp_grid, cmap="coolwarm", shading="auto", transform=ccrs.PlateCarree()
)
plt.colorbar(mesh, label="Temperature (°C)")

plt.title("Enhanced Temperature Heatmap from NetCDF Data")
plt.show()

spark.stop()
