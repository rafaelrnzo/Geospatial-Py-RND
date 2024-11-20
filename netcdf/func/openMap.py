import xarray as xr
import matplotlib.pyplot as plt 
import cartopy.crs as ccrs 

url = 'https://www.ncei.noaa.gov/thredds/dodsC/noaa-global-temp-v5/NOAAGlobalTemp_v5.0.0_gridded_s188001_e202212_c20230108T133308.nc'
xrds = xr.open_dataset(url)

vmin = xrds['anom'].attrs['valid_min']
vmax = xrds['anom'].attrs['valid_max']
vmin_abs = abs(vmin)
abs_max = max(vmin_abs, vmax)

desired_date = '2022-12-01'
data_for_desired_date = xrds.sel(time=desired_date)

plt.figure(figsize=(10,5))
ax = plt.axes(projection=ccrs.PlateCarree())
ax.coastlines()

ax.set_xticks(range(-180,181,60), crs=ccrs.PlateCarree())
ax.set_yticks(range(-90,90,30), crs=ccrs.PlateCarree())
ax.set_xlabel('Longtitude')
ax.set_ylabel('Latitude')

data_for_desired_date['anom'].plot(vmin=-abs_max, vmax=abs_max, cmap="seismic")

plt.title(f'Global Surface Temperatures Anomalies for {desired_date}')
plt.show()