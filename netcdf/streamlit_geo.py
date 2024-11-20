import numpy as np
import matplotlib.pyplot as plt
from netCDF4 import Dataset
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import streamlit as st
import tempfile

# Path to the NetCDF file
file = r'C:\Users\SD-LORENZO-PC\pyproject\rndPy\Geospatial\netcdf\data\1964.nc'

# Streamlit app
st.title("NetCDF Temperature Map Visualization with Cartopy")
st.sidebar.header("Settings")

# Load the NetCDF data
data = Dataset(file, mode='r')

# Extract variables
lats = data.variables['lat'][:]
lons = data.variables['lon'][:]
time = data.variables['time'][:]
tave = data.variables['tave'][:]

# Slider to select time index
time_index = st.sidebar.slider("Select Time Index", 0, len(time) - 1, 0)

# Create the map plot using Cartopy
fig = plt.figure(figsize=(10, 6))
ax = plt.axes(projection=ccrs.PlateCarree())

# Plot temperature data with color gradients
temp_data = np.squeeze(tave[time_index, :, :])
mesh = ax.pcolormesh(lons, lats, temp_data, cmap='jet', transform=ccrs.PlateCarree())

# Add features to the map
ax.coastlines()
ax.add_feature(cfeature.BORDERS, linestyle=':')
ax.add_feature(cfeature.LAND, color='lightgray', alpha=0.5)
ax.add_feature(cfeature.OCEAN, color='lightblue', alpha=0.5)

# Add a color bar
cbar = plt.colorbar(mesh, orientation='vertical', pad=0.05)
cbar.set_label('Average Temperature (°C)')

# Add a title
plt.title(f'Average Temperature on Day {time_index + 1} of 1964')

# Save the plot to a temporary file and display it in Streamlit
with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_file:
    plt.savefig(temp_file.name, format="png", bbox_inches='tight')
    plt.close(fig)
    st.image(temp_file.name, caption=f"Temperature Map for Time Index {time_index}")

# Closing the dataset
data.close()
