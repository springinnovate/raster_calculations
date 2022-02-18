# Calculate the sum of non-nodata values in a raster
import argparse
import glob
import logging

from ecoshard import geoprocessing
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
    nodata = geoprocessing.get_raster_info(raster_path)['nodata'][0]
    for _, block_array in geoprocessing.iterblocks((raster_path, 1)):
        if nodata is not None:
            valid_array = block_array != nodata
        else:
            valid_array = slice(-1)
        running_sum += numpy.sum(block_array[valid_array])
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

    with open('sum_report.csv', 'w') as sum_file:
        sum_file.write('filename,sum\n')
        for raster_path in raster_path_list:
            LOGGER.info(f'***summing {raster_path} please wait...')
            raster_sum = sum_raster(raster_path)
            LOGGER.info(f'{raster_sum} is the sum')
            sum_file.write(f'{raster_path},{raster_sum}\n')

    LOGGER.info('all done')
