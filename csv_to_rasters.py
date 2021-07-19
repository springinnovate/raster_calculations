import math
import glob
import os

from osgeo import gdal
from osgeo import osr
import pygeoprocessing
import numpy

if __name__ == '__main__':
    for path in glob.glob('relative-wealth-index-april-2021/*.csv'):
        target_path = f'{os.path.basename(os.path.splitext(path)[0])}.tif'
        print(target_path)
        with open(path, 'r') as csv_table:
            csv_table.readline()
            lat_array = []
            lng_array = []
            rwi_array = []
            band_per_pixel = True
            for line in csv_table:
                lat, lng, rwi, error = line.split(',')
                lat_array.append(float(lat))
                lng_array.append(float(lng))
                rwi_array.append(float(rwi))
            sorted_lat = sorted(set(lat_array))
            sorted_lng = sorted(set(lng_array))
            min_delta_lat = min([j - i for i, j in zip(sorted_lat[:-1], sorted_lat[1:])])
            min_delta_lng = min([j - i for i, j in zip(sorted_lng[:-1], sorted_lng[1:])])
            max_lat = sorted_lat[-1]
            max_lng = sorted_lng[-1]
            min_lat = sorted_lat[0]
            min_lng = sorted_lng[0]
            cell_length = min(min_delta_lng, min_delta_lat) * 2

            driver = gdal.GetDriverByName('GTiff')
            n_cols = int(math.ceil((sorted_lng[-1]-sorted_lng[0]) / cell_length))
            n_rows = int(math.ceil((sorted_lat[-1]-sorted_lat[0]) / cell_length))
            target_raster = driver.Create(
                target_path, n_cols, n_rows, 1, gdal.GDT_Float32,
                options=(
                    'TILED=YES', 'BIGTIFF=YES', 'COMPRESS=LZW',
                    'BLOCKXSIZE=256', 'BLOCKYSIZE=256'))
            nodata = -9999
            array = numpy.full((n_rows, n_cols), nodata, dtype=numpy.float32)
            target_raster.SetProjection(osr.SRS_WKT_WGS84_LAT_LONG)
            geotransform = [min_lng, cell_length, 0, max_lat, 0, -cell_length]
            inv_gt = gdal.InvGeoTransform(geotransform)
            target_raster.SetGeoTransform(geotransform)
            target_band = target_raster.GetRasterBand(1)
            target_band.SetNoDataValue(nodata)
            for lng, lat, val in zip(lng_array, lat_array, rwi_array):
                i, j = [int(v) for v in gdal.ApplyGeoTransform(inv_gt, lng, lat)]
                try:
                    array[j, i] = val
                except Exception as e:
                    real_lng, real_lat = gdal.ApplyGeoTransform(geotransform, i, j)
                    print(f'cell length{cell_length} {(sorted_lng[-1]-sorted_lng[0])} ')
                    print(f'{e} lng:{lng} lat:{lat} min lng:{sorted_lng[0]} min lat:{sorted_lat[0]} max lng:{sorted_lng[-1]} max lat:{sorted_lat[-1]} real lng:{real_lng} real  lat:{real_lat}')
                    raise
            target_band.WriteArray(array)
            target_band = None
            target_raster = None
