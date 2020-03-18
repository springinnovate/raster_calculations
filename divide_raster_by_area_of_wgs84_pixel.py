"""Divide a raster by the m^2 area of the wgs84 pixel."""
import logging
import math
import sys

from osgeo import gdal
import numpy
import pygeoprocessing

pixel_size = 0.083
lat = 0

logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(processName)s %(levelname)s '
        '%(name)s [%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(__name__)


def area_of_pixel_km2(pixel_size, center_lat):
    """Calculate km^2 area of a wgs84 square pixel.

    Adapted from: https://gis.stackexchange.com/a/127327/2397

    Parameters:
        pixel_size (float): length of side of pixel in degrees.
        center_lat (float): latitude of the center of the pixel. Note this
            value +/- half the `pixel-size` must not exceed 90/-90 degrees
            latitude or an invalid area will be calculated.

    Returns:
        Area of square pixel of side length `pixel_size` centered at
        `center_lat` in km^2.

    """
    a = 6378137  # meters
    b = 6356752.3142  # meters
    e = math.sqrt(1 - (b/a)**2)
    area_list = []
    for f in [center_lat+pixel_size/2, center_lat-pixel_size/2]:
        zm = 1 - e*math.sin(math.radians(f))
        zp = 1 + e*math.sin(math.radians(f))
        area_list.append(
            math.pi * b**2 * (
                math.log(zp/zm) / (2*e) +
                math.sin(math.radians(f)) / (zp*zm)))
    # mult by 1e-6 to convert m^2 to km^2
    return pixel_size / 360. * (area_list[0] - area_list[1]) * 1e-6


def divide_op(num_array, denom_array, nodata):
    result = numpy.empty(shape=num_array.shape)
    result[:] = nodata
    valid_mask = ~numpy.isclose(num_array, nodata)
    result[valid_mask] = num_array[valid_mask] / denom_array[valid_mask]
    return result


if __name__ == '__main__':
    raster_path = (
        'realized_fwfish_distrib_catch_0s_clamped1000_'
        'md5_77c79f3e4366c743fd095ff0d1225d33.tif')
    target_raster = 'per_km_2_%s' % raster_path

    raster_info = pygeoprocessing.get_raster_info(raster_path)

    n_cols, n_rows = raster_info['raster_size']
    gt = raster_info['geotransform']

    lat_area_km2 = numpy.empty(shape=(n_rows, 1))
    for row_index in range(n_rows):
        _, lat_a = gdal.ApplyGeoTransform(gt, 0, row_index)
        _, lat_b = gdal.ApplyGeoTransform(gt, 0, row_index+1)
        lat_center = (lat_a+lat_b)/2.0
        lat_area_km2[row_index][0] = area_of_pixel_km2(
            raster_info['pixel_size'][0], lat_center)

    pygeoprocessing.raster_calculator(
        [(raster_path, 1), lat_area_km2, (raster_info['nodata'][0], 'raw')],
        divide_op, target_raster, raster_info['datatype'],
        raster_info['nodata'][0])
