import numpy as np
import matplotlib.pyplot as plt
from netCDF4 import Dataset
import cartopy.crs as ccrs
import cartopy.feature as cfeature

# Path to NetCDF file
file = r'C:\Users\SD-LORENZO-PC\pyproject\rndPy\Geospatial\netcdf\data\1964.nc'

# Load data
data = Dataset(file, mode='r')
lats = data.variables['lat'][:]
lons = data.variables['lon'][:]
tave = data.variables['tave'][:]

# Select the first day's data
day_data = tave[0, :, :]

# Plot with Cartopy
plt.figure(figsize=(12, 8))
ax = plt.axes(projection=ccrs.PlateCarree())
ax.set_extent([lons.min(), lons.max(), lats.min(), lats.max()], crs=ccrs.PlateCarree())
ax.add_feature(cfeature.COASTLINE)
ax.add_feature(cfeature.BORDERS, linestyle=':')

# Create heatmap
mesh = plt.pcolormesh(lons, lats, day_data, cmap='jet', shading='auto', transform=ccrs.PlateCarree())
plt.colorbar(mesh, label="Average Temperature (°C)")

plt.title('Average Temperature on Day 1 of 1964')
plt.show()
