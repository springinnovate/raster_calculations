"""See python [scriptname] --help"""
import argparse
import logging
import sys

import numpy
from ecoshard import geoprocessing
from ecoshard.geoprocessing.geoprocessing_core import \
    DEFAULT_GTIFF_CREATION_TUPLE_OPTIONS


RASTER_CREATE_OPTIONS = DEFAULT_GTIFF_CREATION_TUPLE_OPTIONS[1]
COG_CREATE_OPTIONS = ('COG', (
            'TILED=YES', 'BIGTIFF=YES', 'COMPRESS=LZW',
            'BLOCKXSIZE=256', 'BLOCKYSIZE=256'))

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


def subtract_op(a_path, b_path, target_nodata):
    a_nodata = geoprocessing.get_raster_info(a_path)['nodata'][0]
    b_nodata = geoprocessing.get_raster_info(b_path)['nodata'][0]
    if target_nodata is None:
        target_nodata = a_nodata

    def _subtract_op(array_a, array_b):
        result = numpy.full(array_a.shape, target_nodata)
        valid_mask = numpy.ones(array_a.shape, dtype=bool)
        if a_nodata is not None:
            valid_mask &= array_a == a_nodata

        if b_nodata is not None:
            valid_mask &= array_b == b_nodata
        result[valid_mask] = array_a[valid_mask]-array_b[valid_mask]
        return result


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

    if args.cog:
        raster_create_options = COG_CREATE_OPTIONS
    else:
        raster_create_options = RASTER_CREATE_OPTIONS

    raster_info = geoprocessing.get_raster_info(args.raster_A_path)
    geoprocessing.raster_calculator(
        [(args.raster_A_path, 1), (args.raster_B_path, 1)],
        subtract_op(
            args.raster_A_path, args.raster_B_path, args.target_nodata),
        args.target_raster_path, raster_info['datatype'],
        raster_create_options=raster_create_options)


if __name__ == '__main__':
    main()
