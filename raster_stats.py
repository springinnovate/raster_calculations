"""Calculate raster statistics.

Summarize by pixel count.
Filter by polygon.
Show real and degree area.
"""
import argparse
import collections
import logging
import math
import os

from osgeo import gdal
from osgeo import osr
from ecoshard.geoprocessing import get_raster_info
from ecoshard.geoprocessing import iterblocks
import numpy
gdal.SetCacheMax(2**27)

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.WARN)


def _area_of_pixel(pixel_size, center_lat):
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


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(description='Calculate raster stats.')
    parser.add_argument('raster_path', help='path to raster')
    args = parser.parse_args()

    raster_info = get_raster_info(args.raster_path)
    raster_srs = osr.SpatialReference()
    raster_srs.ImportFromWkt(raster_info['projection_wkt'])
    LOGGER.debug(f'projected: {raster_srs.IsProjected()}')
    pixel_area = abs(numpy.prod(raster_info['pixel_size']))
    target_csv_path = \
        f'{os.path.basename(os.path.splitext(args.raster_path)[0])}.csv'

    with open(target_csv_path, 'w') as csv_file:
        csv_file.write('pixel_value,pixel_count,area (raster units)')
        if not raster_srs.IsProjected():
            csv_file.write(',area m^2')
        csv_file.write('\n')

    pixel_stat_dict = collections.defaultdict(int)
    area_stat_dict = collections.defaultdict(float)

    for offset_info, block_data in iterblocks((args.raster_path, 1)):
        unique_vals, frequency = numpy.unique(block_data, return_counts=True)
        for val, count in zip(unique_vals, frequency):
            pixel_stat_dict[val] += count

        if not raster_srs.IsProjected():
            n_rows = offset_info['win_ysize']
            pixel_height = abs(raster_info['geotransform'][5])
            # the / 2 is to get in the center of the pixel
            _, y_origin = gdal.ApplyGeoTransform(
                raster_info['geotransform'], 0, offset_info['yoff'])
            _, y_bot = gdal.ApplyGeoTransform(
                raster_info['geotransform'], 0,
                offset_info['yoff']+offset_info['win_ysize'])

            miny = y_origin + pixel_height/2
            maxy = y_bot + pixel_height/2
            lat_vals = numpy.linspace(maxy, miny, n_rows)
            deg_area_vals = numpy.expand_dims(numpy.array(
                [_area_of_pixel(pixel_height, val) for val in lat_vals]),
                axis=1)

            for val in unique_vals:
                area_stat_dict[val] += numpy.sum(
                    (block_data == val) * deg_area_vals)

    with open(target_csv_path, 'a') as csv_file:
        for val in sorted(pixel_stat_dict):
            csv_file.write(
                f'{val},{pixel_stat_dict[val]},'
                f'{pixel_area*pixel_stat_dict[val]}')
            if not raster_srs.IsProjected():
                csv_file.write(f',{area_stat_dict[val]}')
            csv_file.write('\n')


if __name__ == '__main__':
    main()
