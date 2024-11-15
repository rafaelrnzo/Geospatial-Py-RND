from osgeo import gdal
import subprocess
import os
import richdem as rd
import matplotlib.pyplot as plt

# Set up file paths
input_dem_path = "nepal.tif"
output_slope1_path = "slope0.tif"
output_slope2_path = "slope2.tif"
output_slope3_path = "slope3.tif"

try:
    # Using os and subprocess to calculate slope and generate output raster
    cmd = f"gdaldem slope {input_dem_path} {output_slope1_path} -compute_edges"  # Adjust command as needed
    subprocess.check_call(cmd.split())

    # Open the first slope result and load it as an array
    slp1 = gdal.Open(output_slope1_path)
    slp1_array = slp1.GetRasterBand(1).ReadAsArray()

    # Using GDAL's DEMProcessing for slope calculation
    dem = gdal.Open(input_dem_path)
    slp2 = gdal.DEMProcessing(output_slope2_path, dem, "slope", computeEdges=True)
    slp2_array = slp2.GetRasterBand(1).ReadAsArray()

    # Close GDAL datasets
    slp1 = None
    slp2 = None
    dem = None

    # Using richdem to calculate slope
    dem = rd.LoadGDAL(input_dem_path)
    slp3 = rd.TerrainAttribute(dem, attrib="slope_degrees")

    # Save slope result using richdem with explicit GTiff driver
    rd.SaveGDAL(output_slope3_path, slp3, driver='GTiff')

    # Visualize the slope array generated from GDAL's DEMProcessing
    plt.figure()
    plt.imshow(slp2_array, cmap='terrain')
    plt.colorbar(label='Slope')
    plt.title("Slope (from GDAL DEMProcessing)")
    plt.show()

except subprocess.CalledProcessError as e:
    print(f"Error in executing GDAL command: {e}")
except AttributeError as e:
    print(f"Attribute error encountered: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
finally:
    # Close all datasets to ensure memory is freed up
    dem = None
