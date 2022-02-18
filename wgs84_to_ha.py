"""Generate a per pixel ha area raster from an arbitrary wgs84 raster."""
import argparse
import logging
import math
import sys

from ecoshard import geoprocessing
from osgeo import gdal
import numpy


gdal.SetCacheMax(2**26)

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


def raster_to_area_raster(base_raster_path, target_raster_path):
    """Convert base to a target raster of same shape with per area pixels."""
    base_raster_info = geoprocessing.get_raster_info(base_raster_path)

    # create 1D array of pixel size vs. lat
    n_rows = base_raster_info['raster_size'][1]
    pixel_height = abs(base_raster_info['geotransform'][5])
    # the / 2 is to get in the center of the pixel
    miny = base_raster_info['bounding_box'][1] + pixel_height/2
    maxy = base_raster_info['bounding_box'][3] - pixel_height/2
    lat_vals = numpy.linspace(maxy, miny, n_rows)

    pixel_area_per_lat = 1.0 / 10000.0 * numpy.array([
        [area_of_pixel(pixel_height, lat_val)] for lat_val in lat_vals])

    geoprocessing.raster_calculator(
        [(base_raster_path, 1), pixel_area_per_lat],
        lambda x, y: y, target_raster_path,
        gdal.GDT_Float32, -1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Create a ha/pixel raster from a base.')
    parser.add_argument(
        'base_raster_path',
        help='Path to base raster whose projection is lat/lng')
    parser.add_argument(
        'target_raster_path',
        help='Path to desired target raster with value of ha/pixel.')
    args = parser.parse_args()
    raster_to_area_raster(args.base_raster_path, args.target_raster_path)