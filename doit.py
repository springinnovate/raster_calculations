import os
import glob

import gdal
import pygeoprocessing

if __name__ == '__main__':
    gtiff_driver = gdal.GetDriverByName("GTiff")
    for path in glob.glob('data/*/*/*/*.nc'):
        print(path)
        raster_info = pygeoprocessing.get_raster_info(path)
        print(raster_info['n_bands'])
        raster = gdal.OpenEx(path, gdal.OF_RASTER)
        target_path = os.path.join('output', f'{os.path.basename(os.path.splitext(path)[0])}.tif')
        gtiff_driver.CreateCopy(target_path, raster)
