"""See python [scriptname] --help"""
import argparse
import logging
import os
import sys

from osgeo import gdal
from ecoshard import geoprocessing
from ecoshard.geoprocessing.geoprocessing_core import \
    DEFAULT_GTIFF_CREATION_TUPLE_OPTIONS
import numpy

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


def subtract_op(a_nodata, b_nodata, target_nodata):
    def _subtract_op(array_a, array_b):
        result = numpy.full(array_a.shape, target_nodata)
        valid_mask = numpy.ones(array_a.shape, dtype=bool)
        if a_nodata is not None:
            valid_mask &= array_a == a_nodata

        if b_nodata is not None:
            valid_mask &= array_b == b_nodata
        result[valid_mask] = array_a[valid_mask]-array_b[valid_mask]
        return result
    return _subtract_op


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(description=(
        'Subtract two rasters (A-B) of equal size and projection'))
    parser.add_argument('raster_A_path', help='Path to raster A.')
    parser.add_argument('raster_B_path', help='Path to raster B.')
    parser.add_argument('target_raster_path', help='Path to target raster')
    parser.add_argument(
        '--cog', action='store_true',
        help='set if desired output is COG format')
    parser.add_argument('--target_nodata', type=float, help=(
        'If set, use this nodata, otherwise use target nodata value from '
        'raster A'))
    args = parser.parse_args()

    raster_info = geoprocessing.get_raster_info(args.raster_A_path)

    a_nodata = geoprocessing.get_raster_info(args.raster_A_path)['nodata'][0]
    b_nodata = geoprocessing.get_raster_info(args.raster_A_path)['nodata'][0]
    target_nodata = args.target_nodata
    if target_nodata is None:
        target_nodata = a_nodata

    geoprocessing.single_thread_raster_calculator(
        [(args.raster_A_path, 1), (args.raster_B_path, 1)],
        subtract_op(a_nodata, b_nodata, target_nodata),
        args.target_raster_path, raster_info['datatype'], target_nodata)

    if args.cog:
        cog_driver = gdal.GetDriverByName('COG')
        base_raster = gdal.OpenEx(args.target_raster_path, gdal.OF_RASTER)
        cog_file_path = os.path.join(
            f'cog_{os.path.basename(args.target_raster_path)}')
        options = ('COMPRESS=LZW', 'NUM_THREADS=ALL_CPUS', 'BIGTIFF=YES')
        LOGGER.info(f'convert {args.target_raster_path} to COG {cog_file_path} with {options}')
        cog_raster = cog_driver.CreateCopy(
            cog_file_path, base_raster, options=options,
            callback=geoprocessing._make_logger_callback(
                f"COGing {cog_file_path} %.1f%% complete %s"))
        del cog_raster


if __name__ == '__main__':
    main()
