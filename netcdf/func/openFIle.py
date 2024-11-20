import xarray as xr
import matplotlib.pyplot as plt

# netcdf_file = 'https://opendap1.nodc.no/opendap/physics/point/cruise/nansen_legacy-single_profile/NMDC_Nansen-Legacy_PR_CT_58US_2021708/CTD_station_P1_NLEG01-1_-_Nansen_Legacy_Cruise_-_2021_Joint_Cruise_2-1.nc'
# xrds = xr.open_dataset(netcdf_file)

# print(xrds.attrs['Conventions'])

# dimensions = xrds.dims
# coords = xrds.coords

# print(coords)

url = 'https://opendap1.nodc.no/opendap/chemistry/point/cruise/nansen_legacy/2021708/Chlorophyll_A_and_phaeopigments_Nansen_Legacy_cruise_2021708_station_P4_NLEG11_20210718T085042.nc'
xrds = xr.open_dataset(url)

fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(12,6))
# xr.plot.line(xrds['CHLOROPHYLL_A_TOTAL'].dropna('DEPTH'), y='DEPTH', yincrease=False)
xrds['CHLOROPHYLL_A_TOTAL'].plot.line(y='DEPTH', yincrease=False, label='TOTAL', ax=axes[0])
xrds['CHLOROPHYLL_A_10um'].plot.line(y='DEPTH', yincrease=False, label='10um', ax=axes[0])
axes[0].set_title('Chlorophyll a')
axes[0].legend()

xrds['PHAEOPIGMENTS_TOTAL'].plot.line(y='DEPTH', yincrease=False, label='TOTAL', ax=axes[1])
xrds['PHAEOPIGMENTS_10um'].plot.line(y='DEPTH', yincrease=False, label='10um', ax=axes[1])
axes[1].set_title('Phaeopigments')
axes[1].legend()

plt.suptitle('Chlorophyll and Phaeopigments')
plt.tight_layout(rect=[0, 0, 1, 0.96])
plt.show()
