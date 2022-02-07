"""Calculate area of a mask."""
import argparse
import math

import logging
import sys

from ecoshard import geoprocessing
from osgeo import gdal
from osgeo import osr
import numpy


gdal.SetCacheMax(2**27)

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)

LOGGER = logging.getLogger(__name__)


def area_of_pixel(pixel_size, center_lat):
    """Calculate m^2 area of a wgs84 square pixel.

    Adapted from: https://gis.stackexchange.com/a/127327/2397

    Args:
        pixel_size (float): length of side of pixel in degrees.
        center_lat (float): latitude of the center of the pixel. Note this
            value +/- half the `pixel-size` must not exceed 90/-90 degrees
            latitude or an invalid area will be calculated.

    Returns:
        Area of square pixel of side length `pixel_size` centered at
        `center_lat` in m^2.

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
    return abs(pixel_size / 360. * (area_list[0] - area_list[1]))


def mask_op(mask_array, value_array):
    """Mask out value to 0 if mask array is not 1."""
    result = numpy.copy(value_array)
    result[mask_array != 1] = 0.0
    return result


def calculate_mask_area(base_mask_raster_path):
    """Calculate area of mask==1."""
    base_raster_info = geoprocessing.get_raster_info(
        base_mask_raster_path)

    base_srs = osr.SpatialReference()
    base_srs.ImportFromWkt(base_raster_info['projection_wkt'])
    if base_srs.IsProjected():
        # convert m^2 of pixel size to Ha
        pixel_conversion = numpy.array([[
            abs(base_raster_info['pixel_size'][0] *
                base_raster_info['pixel_size'][1])]]) / 10000.0
    else:
        # create 1D array of pixel size vs. lat
        n_rows = base_raster_info['raster_size'][1]
        pixel_height = abs(base_raster_info['geotransform'][5])
        # the / 2 is to get in the center of the pixel
        miny = base_raster_info['bounding_box'][1] + pixel_height/2
        maxy = base_raster_info['bounding_box'][3] - pixel_height/2
        lat_vals = numpy.linspace(maxy, miny, n_rows)

        pixel_conversion = 1.0 / 10000.0 * numpy.array([
            [area_of_pixel(pixel_height, lat_val)] for lat_val in lat_vals])

    nodata = base_raster_info['nodata'][0]
    area_raster_path = 'tmp_area_mask.tif'
    geoprocessing.raster_calculator(
        [(base_mask_raster_path, 1), pixel_conversion], mask_op,
        area_raster_path, gdal.GDT_Float32, nodata)

    area_sum = 0.0
    for _, area_block in geoprocessing.iterblocks((area_raster_path, 1)):
        area_sum += numpy.sum(area_block)
    return area_sum


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Calculate area of pixel mask.')
    parser.add_argument(
        'input_mask',
        help='Path to a mask pixels are 1 or not 1.')
    args = parser.parse_args()

    LOGGER.info(f'calculating area of pixels that are 1 in {args.input_mask}')
    mask_area = calculate_mask_area(args.input_mask)
    LOGGER.info(f'calculated area for {args.input_mask} is {mask_area}Ha')
