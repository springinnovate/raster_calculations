"""Calculate (Raster A - Raster B) but treat nodata as 0'."""
import argparse
import logging
import os
import shutil
import time

from ecoshard import geoprocessing
import numpy

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(__name__)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='list unique values in raster')
    parser.add_argument('raster_path', help='Path to raster')
    args = parser.parse_args()

    unique_set = set()
    n_blocks = len(list(geoprocessing.iterblocks((args.raster_path, 1), largest_block=2**20, offset_only=True)))
    blocks_left = n_blocks
    last_time = time.time()
    for _, array in geoprocessing.iterblocks((args.raster_path, 1), largest_block=2**20):
        unique_set |= set(numpy.unique(array))
        blocks_left -= 1
        if time.time()-last_time > 5.0:
            LOGGER.info(f'{(n_blocks-blocks_left)/n_blocks*100:.2f}% complete')
            last_time = time.time()
    set_string = "\n".join([str(x) for x in sorted(unique_set)])
    LOGGER.info(f'unique values in {args.raster_path}:\n{set_string}')
    