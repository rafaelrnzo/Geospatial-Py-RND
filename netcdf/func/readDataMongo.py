import pymongo
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import numpy as np
import pandas as pd

mongo_uri = "mongodb+srv://devadmin:2553669874123654@dev218.kke0v.mongodb.net/?retryWrites=true&w=majority&appName=dev218"
database_name = "climate_data"
collection_name = "temperature_data"

client = pymongo.MongoClient(mongo_uri)
db = client[database_name]
collection = db[collection_name]

data_from_mongo = list(collection.find({}, {"_id": 0}))  

df = pd.DataFrame(data_from_mongo)

print("Data fetched from MongoDB:")
print(df.head())

lats = df['lat'].values
lons = df['lon'].values
anoms = df['anom'].values

lon_grid, lat_grid = np.meshgrid(np.unique(lons), np.unique(lats))

anom_grid = np.reshape(anoms, lon_grid.shape)

plt.figure(figsize=(10, 5))

ax = plt.axes(projection=ccrs.PlateCarree())

ax.coastlines()
ax.set_xticks(range(-180, 181, 60), crs=ccrs.PlateCarree())
ax.set_yticks(range(-90, 90, 30), crs=ccrs.PlateCarree())
ax.set_xlabel('Longitude')
ax.set_ylabel('Latitude')

c = ax.contourf(lon_grid, lat_grid, anom_grid, levels=20, cmap="seismic", transform=ccrs.PlateCarree())

plt.colorbar(c, label='Temperature Anomaly (°C)')

plt.title('Temperature Anomalies from MongoDB')

plt.show()

client.close()
