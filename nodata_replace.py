"""Replace nodata values in raster A with values in raster B."""
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


def replace_a_with_b(array_a, array_b, a_nodata, b_nodata):
    """Set array_a to array_b where array_a==nodata an array_b != nodata."""
    result = numpy.copy(array_a)
    valid_mask = (
        (numpy.isclose(array_a, a_nodata) |
         numpy.isnan(array_a)) &
        ~numpy.isclose(array_b, b_nodata) &
        ~numpy.isnan(array_b))
    result[valid_mask] = array_b[valid_mask]
    result[numpy.isnan(result)] = a_nodata
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=(
        'Replace nodata from raster A with values in raster B. '
        'if raster B also has nodata values then keep A the same.'))
    parser.add_argument('raster_a_path', type=str, help='path to raster A')
    parser.add_argument('raster_b_path', type=str, help='path to raster B')
    parser.add_argument(
        'target_path', type=str, help='path to desired output target')
    parser.add_argument(
        '--overwrite', action='store_true',
        help='overwrite target raster if it exists')

    args = parser.parse_args()

    if os.path.exists(args.target_path) and not args.overwrite:
        LOGGER.error(
            f'the target {args.target_path} exists. to overwrite this, add '
            'the --overwrite flag and run again')
        sys.exit(-1)

    raster_a_info = geoprocessing.get_raster_info(args.raster_a_path)
    a_nodata = raster_a_info['nodata'][0]
    raster_b_info = geoprocessing.get_raster_info(args.raster_b_path)
    b_nodata = raster_b_info['nodata'][0]

    LOGGER.info(
        f'replacing {a_nodata} in {args.raster_a_path} with values in '
        f'{args.raster_b_path} and writing to {args.target_path}')

    geoprocessing.raster_calculator(
        [(args.raster_a_path, 1), (args.raster_b_path, 1),
         (a_nodata, 'raw'),
         (b_nodata, 'raw')], replace_a_with_b,
        args.target_path, raster_a_info['datatype'], a_nodata)
