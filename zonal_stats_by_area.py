"""Calculate real area of raster mask under polygon."""
import argparse
import logging
import math
import os
import sys

from osgeo import gdal
from ecoshard import geoprocessing
import numpy

gdal.SetCacheMax(2**26)

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)

LOGGER = logging.getLogger(__name__)

WORKSPACE_DIR = 'zonal_stats_workspace'
os.makedirs(WORKSPACE_DIR, exist_ok=True)


def _area_of_pixel_km2(pixel_size, center_lat):
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


def _get_area_column(raster_info):
    # create lat/lng area column
    n_cols, n_rows = raster_info['raster_size']
    gt = raster_info['geotransform']

    lat_area_km2 = numpy.empty(shape=(n_rows, 1))
    for row_index in range(n_rows):
        _, lat_a = gdal.ApplyGeoTransform(gt, 0, row_index)
        _, lat_b = gdal.ApplyGeoTransform(gt, 0, row_index+1)
        lat_center = (lat_a+lat_b)/2.0
        lat_area_km2[row_index][0] = _area_of_pixel_km2(
            raster_info['pixel_size'][0], lat_center)
    return lat_area_km2


def _mult_by_mask_op(base_array, area_array, base_nodata):
    # mult base by array but keep nodata
    if base_nodata is not None:
        valid_mask = base_array != base_nodata
    else:
        valid_mask = numpy.ones(base_array.shape, dtype=bool)
    result = numpy.full(base_array.shape, -1, dtype=numpy.float32)
    result[valid_mask] = base_array[valid_mask] * area_array[valid_mask]
    return result


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(
        description='Calculate real area of raster mask under polygon.')
    parser.add_argument('raster_path', help='Raster path')
    parser.add_argument('vector_path', help='Vector path')
    args = parser.parse_args()

    raster_info = geoprocessing.get_raster_info(args.raster_path)
    # create a masked area raster
    lat_area_km2 = _get_area_column(raster_info)
    masked_area_raster_path = os.path.join(
        WORKSPACE_DIR, f'area_km_{os.path.basename(args.raster_path)}')
    LOGGER.debug(lat_area_km2)
    geoprocessing.raster_calculator(
        [(args.raster_path, 1), lat_area_km2,
         (raster_info['nodata'][0], 'raw')], _mult_by_mask_op,
        masked_area_raster_path, gdal.GDT_Float32, -1)

    # mask lat/lng area column by raster_path
    projected_vector_path = os.path.join(
        WORKSPACE_DIR, f'projected_{os.path.basename(args.vector_path)}')

    geoprocessing.reproject_vector(
        args.vector_path, raster_info['projection_wkt'],
        projected_vector_path, driver_name='GPKG', copy_fields=True)

    # * proportional area in terms of real area of 1 vs non-1 under each polygon
    # * # of pixels

    # base stats
    stats = geoprocessing.zonal_statistics(
        (args.raster_path, 1), projected_vector_path,
        polygons_might_overlap=False, working_dir='.')

    # area stats

    LOGGER.debug(stats)


if __name__ == '__main__':
    main()
