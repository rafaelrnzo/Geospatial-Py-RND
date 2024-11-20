import PIL 
import PIL.Image
import numpy as np

image_frames = []

days = np.arange(1,366)

for k in days:
    new_frame = PIL.Image.open(r'C:\Users\SD-LORENZO-PC\pyproject\rndPy\Geospatial\netcdf\data\output'+'\\' + "Day_"+str(k) + '.jpg')
    image_frames.append(new_frame)
    
image_frames[0].save('temperatur_timelapse2.gif', format = 'GIF',
                     append_images = image_frames[1: ],
                     save_all = True, duration = 200,
                     loop = 0
                     )