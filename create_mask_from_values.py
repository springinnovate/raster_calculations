"""Create a 0/1 mask given a list of values to be 1s."""
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
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


def mask_by_values_op(value_list, nodata, target_nodata, invert):
    def _mask_by_values_op(array):
        result = numpy.isin(array, value_list).astype(numpy.byte)
        if invert:
            result = ~result
        if nodata is not None:
            result[value_list == nodata] = target_nodata
        return result
    return _mask_by_values_op


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=(
        'Create a 0/1 mask given a list of values to be 1s'))
    parser.add_argument('base_raster_path', help='Path to the base raster.')
    parser.add_argument('--value_list', nargs='+', type=float, help=(
        'A list of values in base_raster_path to convert to 1.'),
        required=True)
    parser.add_argument(
        '--target_raster_path', help='Path to the raster to create.',
        required=True)
    parser.add_argument(
        '--invert', action='store_true',
        help='make a 0 where values are rather than 1')
    parser.add_argument(
        '--force', action='store_true', help='set to overwrite output')
    args = parser.parse_args()

    if os.path.exists(args.target_raster_path) and not args.force:
        raise ValueError(
            f'{args.target_raster_path} already exists, will not overwrite.')

    raster_info = geoprocessing.get_raster_info(args.base_raster_path)
    nodata = raster_info['nodata'][0]
    target_nodata = 2
    # count valid pixels
    print(args.base_raster_path)
    geoprocessing.raster_calculator(
        [(args.base_raster_path, 1)],
        mask_by_values_op(args.value_list, nodata, target_nodata, args.invert),
        args.target_raster_path, raster_info['datatype'],
        target_nodata, allow_different_blocksize=True)

    LOGGER.info(f'all done, result at {args.target_raster_path}')
