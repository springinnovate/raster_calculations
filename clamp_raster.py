"""Clamp raster to bounds."""
import argparse
import logging
import os
import sys

from ecoshard import geoprocessing
import numpy

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=(
        'Clamp raster A to lower and/or upper layer.'))
    parser.add_argument('raster_path', type=str, help='path to input raster')
    parser.add_argument('--lower', type=float, help='lower bound to clamp')
    parser.add_argument('--upper', type=float, help='upper bound to clamp')
    args = parser.parse_args()
    suffix = '_clamped'
    if args.lower is not None:
        suffix += f'_l{args.lower}'
    if args.upper is not None:
        suffix += f'_u{args.upper}'

    target_raster_path = f'%s{suffix}%s' % os.path.splitext(args.raster_path)

    def _clamp_op(array):
        result = array.copy()
        if args.lower is not None:
            result[array < args.lower] = args.lower
        if args.upper is not None:
            result[array > args.upper] = args.upper
        return result

    LOGGER.info(f'clipping to {target_raster_path}')
    raster_info = geoprocessing.get_raster_info(args.raster_path)
    geoprocessing.raster_calculator(
        [(args.raster_path, 1)], _clamp_op,
        target_raster_path, raster_info['datatype'],
        raster_info['nodata'][0])
