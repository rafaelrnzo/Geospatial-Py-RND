import numpy as np
import matplotlib.pyplot as plt
from netCDF4 import Dataset
from mpl_toolkits.basemap import Basemap

file = r'C:\Users\SD-LORENZO-PC\pyproject\rndPy\Geospatial\netcdf\data\1964.nc'

data = Dataset(file, mode='r')

lats = data.variables['lat'][:]
lons = data.variables['lon'][:]
time = data.variables['time'][:]
tave = data.variables['tave'][:]

mp = Basemap(projection='merc',
             llcrnrlat=lats.min(), urcrnrlat=lats.max(),
             llcrnrlon=lons.min(), urcrnrlon=lons.max(),
             resolution='l')

lon, lat = np.meshgrid(lons, lats)
x, y = mp(lon, lat)

days = np.arange(0, 1)  

for i in days:
    plt.figure(figsize=(12, 8))

    levels = np.linspace(-40, 40, 100)  # Define consistent levels
    c_scheme = mp.contourf(x, y, np.squeeze(tave[i, :, :]), cmap='jet', levels=levels)

    mp.drawcoastlines()
    mp.drawcountries()
    mp.drawparallels(np.arange(-90., 91., 10.), labels=[1, 0, 0, 0])
    mp.drawmeridians(np.arange(-180., 181., 10.), labels=[0, 0, 0, 1])

    cbar = mp.colorbar(c_scheme, location='right', pad='10%')
    cbar.set_label('Average Temperature (°C)')

    day = i + 1
    plt.title(f'Average Temperature on Day {day} of Year 1964')

    plt.savefig(rf'C:\Users\SD-LORENZO-PC\pyproject\rndPy\Geospatial\netcdf\data\output\{day}.jpg')
    plt.clf()

print("All plots saved successfully!")
