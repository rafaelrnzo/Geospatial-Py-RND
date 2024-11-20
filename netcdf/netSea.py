import matplotlib.pyplot as plt
import numpy as np
from netCDF4 import Dataset
from mpl_toolkits.basemap import Basemap

file = r'C:\Users\SD-LORENZO-PC\pyproject\rndPy\Geospatial\netcdf\data\sea.nc'

data = Dataset(file, mode='r')

lats = data.variables['lat'][:]
lons = data.variables['lon'][:]
sst = data.variables['sst'][:]

# Create a Basemap instance
mp = Basemap(projection="merc",
             llcrnrlat=lats.min(), urcrnrlat=lats.max(),
             llcrnrlon=lons.min(), urcrnrlon=lons.max(),
             resolution='l')

lon, lat = np.meshgrid(lons, lats)
x, y = mp(lon, lat)

# Set figure size
plt.figure(figsize=(16, 10))

# Draw a filled contour plot with finer details
c_scheme = mp.contourf(x, y, np.squeeze(sst[0, :, :]), levels=150, cmap='coolwarm')

# Add coastlines, countries, and grid lines with custom styling
mp.drawcoastlines(color='black', linewidth=0.5)
mp.drawcountries(color='black', linewidth=0.5)
mp.drawparallels(np.arange(-90., 91., 20.), labels=[1, 0, 0, 0], fontsize=10, linewidth=0.3, color='gray')
mp.drawmeridians(np.arange(-180., 181., 30.), labels=[0, 0, 0, 1], fontsize=10, linewidth=0.3, color='gray')

# Add colorbar with enhanced styling
cbar = mp.colorbar(c_scheme, location='right', pad='5%')
cbar.set_label('Sea Surface Temperature (°C)', fontsize=12)
cbar.ax.tick_params(labelsize=10)

# Add a title with improved font size and spacing
plt.title('Sea Surface Temperature', fontsize=16, fontweight='bold', pad=20)

# Show the plot
plt.tight_layout()
plt.show()
