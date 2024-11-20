import xarray as xr
import numpy as np
import datetime as dt

url = 'https://www.ncei.noaa.gov/thredds/dodsC/noaa-global-temp-v5/NOAAGlobalTemp_v5.0.0_gridded_s188001_e202212_c20230108T133308.nc'
ds = xr.open_dataset(url)
df = ds.to_dataframe()

timeslice = ds.sel(time = dt.datetime(2020,12,1), method='nearest')
# timeslice.to_csv(r'C:\Users\SD-LORENZO-PC\pyproject\rndPy\Geospatial\netcdf\dataTime.csv')

dfTime = timeslice.to_dataframe()

dfTime.to_csv(r'C:\Users\SD-LORENZO-PC\pyproject\rndPy\Geospatial\netcdf\data.csv')
# dfTime.to_excel(r'C:\Users\SD-LORENZO-PC\pyproject\rndPy\Geospatial\netcdf\temp_anom.xlsx')
print(df)