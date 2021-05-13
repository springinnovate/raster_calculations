# Calculate the sum of non-nodata values in a raster
import argparse
import glob
import logging

import pygeoprocessing
import numpy

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(__name__)

def sum_raster(raster_path):
    """Return the sum of non-nodata value pixels in ``raster_path``."""
    running_sum = 0.0
    nodata = pygeoprocessing.get_raster_info(raster_path)['nodata'][0]
    for _, block_array in pygeoprocessing.iterblocks((raster_path, 1)):
        if nodata is not None:
            valid_array = block_array != nodata
        else:
            valid_array = slice(-1)
        running_sum = numpy.sum(block_array[valid_array])
    return running_sum


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Sum rasters and report values.')
    parser.add_argument(
        'raster_list', nargs='+',
        help='List of raster paths or wildcards to sum.')

    args = parser.parse_args()

    raster_path_list = (
        raster_path for raster_glob in args.raster_list
        for raster_path in glob.glob(raster_glob)
        )

    for raster_path in raster_path_list:
        LOGGER.info(f'***summing {raster_path} please wait...')
        LOGGER.info(f'{sum_raster(raster_path)} is the sum')

    LOGGER.info('all done')
