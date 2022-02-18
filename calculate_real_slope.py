"""Calculate slope in any projection."""
import argparse
import logging
import os
import sys
import tempfile

from ecoshard import geoprocessing
from osgeo import gdal
import numpy
import shutil

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(processName)s %(levelname)s '
        '%(name)s [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


def mult_op(base_array, base_nodata, factor_array, target_nodata):
    """Multipy base by a constant factor array."""
    result = numpy.empty(base_array.shape, dtype=numpy.float32)
    result[:] = target_nodata
    valid_mask = ~numpy.isclose(base_array, base_nodata)
    result[valid_mask] = base_array[valid_mask] * factor_array[valid_mask]
    return result


def degrees_per_meter(max_lat, min_lat, n_pixels):
    """Calculate degrees/meter for a range of latitudes.

    Create a 1D array ranging from "max_lat" to "min_lat" where each element
    contains an average degrees per meter that maps to that lat value.

    Adapted from: https://gis.stackexchange.com/a/127327/2397

    Args:
        max_lat (float): max lat for first element
        min_lat (float): min lat non-inclusive for last element
        n_pixels (int): number of elements in target array

    Returns:
        Area of square pixel of side length `pixel_size_in_degrees` centered at
        `center_lat` in m^2.

    """
    m1 = 111132.92
    m2 = -559.82
    m3 = 1.175
    m4 = -0.0023
    p1 = 111412.84
    p2 = -93.5
    p3 = 0.118

    meters_per_degree = numpy.empty(n_pixels)
    for index, lat_deg in enumerate(numpy.linspace(
            max_lat, min_lat, num=n_pixels, endpoint=False)):
        lat = lat_deg * numpy.pi / 180

        lat_mpd = (
            m1+(m2*numpy.cos(2*lat))+(m3*numpy.cos(4*lat)) +
            (m4*numpy.cos(6*lat)))
        lng_mpd = (
            (p1*numpy.cos(lat))+(p2*numpy.cos(3*lat)) + (p3*numpy.cos(5*lat)))

        meters_per_degree[index] = numpy.sqrt(lat_mpd*lng_mpd)

    return 1./meters_per_degree


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Calculate real slope.')
    parser.add_argument('dem_path', help='path to dem')
    parser.add_argument(
        'slope_path', help='output path to slope raster created by this call')
    parser.add_argument(
        '--dem_in_degrees', help='use this if dem SRS is in degrees',
        action='store_true')

    args = parser.parse_args()

    if args.dem_in_degrees:
        dem_info = geoprocessing.get_raster_info(args.dem_path)
        bb = dem_info['bounding_box']
        n_cols, n_rows = dem_info['raster_size']

        # create meters to degree array
        lng_m_to_d_array = degrees_per_meter(bb[3], bb[1], n_rows)

        work_dir = tempfile.mkdtemp(dir='.', prefix='slope_working_dir')
        dem_in_degrees_raster_path = os.path.join(
            work_dir, 'dem_in_degrees.tif')
        nodata = -9999
        geoprocessing.raster_calculator(
            [(args.dem_path, 1), (dem_info['nodata'][0], 'raw'),
             lng_m_to_d_array[:, None], (nodata, 'raw')],
            mult_op, dem_in_degrees_raster_path, gdal.GDT_Float32,
            nodata)
        dem_raster_path = dem_in_degrees_raster_path
    else:
        dem_raster_path = args.dem_path

    geoprocessing.calculate_slope(
        (dem_raster_path, 1), args.slope_path)

    if args.dem_in_degrees:
        shutil.rmtree(work_dir)
