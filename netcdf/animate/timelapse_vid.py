import cv2
import numpy as np

image_folder = r'C:\Users\SD-LORENZO-PC\pyproject\rndPy\Geospatial\netcdf\data\output'
output_video = 'temperature_timelapse23.mp4'

days = np.arange(1, 366)
image_files = [f"{image_folder}\\Day_{k}.jpg" for k in days]

first_frame = cv2.imread(image_files[0])
height, width, layers = first_frame.shape

fourcc = cv2.VideoWriter_fourcc(*'mp4v')
video = cv2.VideoWriter(output_video, fourcc, 5, (width, height))

for image_file in image_files:
    frame = cv2.imread(image_file)
    if frame is not None:
        video.write(frame)
    else:
        print(f"Error loading {image_file}")

video.release()
print(f"Video saved as {output_video}")

