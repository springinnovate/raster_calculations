"""Mask a raster to be nodata where another raster is nodata."""
import argparse
import glob
import os
import multiprocessing
import logging

import numpy
from osgeo import gdal
from ecoshard import taskgraph
from ecoshard import geoprocessing
from ecoshard.geoprocessing import VECTOR_TYPE
from ecoshard.geoprocessing import RASTER_TYPE

gdal.SetCacheMax(2**27)

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))

LOGGER = logging.getLogger(__name__)

WORKSPACE_DIR = 'mask_by_raster_workspace'


def mask_raster(base_raster_path, mask_raster_path, target_raster_path):
    """Mask base by mask such that any nodata in mask is set to nodata in base."""
    base_info = geoprocessing.get_raster_info(base_raster_path)
    base_nodata = base_info['nodata'][0]
    mask_nodata = geoprocessing.get_raster_info(mask_raster_path)['nodata'][0]
    def mask_op(base_array, mask_array):
        result = numpy.copy(base_array)
        result[mask_array == mask_nodata] = base_nodata
        return result

    geoprocessing.single_thread_raster_calculator(
        [(base_raster_path, 1), (mask_raster_path, 1)], mask_op,
        target_raster_path, base_info['datatype'], base_nodata)


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(description=(
        'Mask raster by another raster. Results are in a subdirectory called '
        f'"{WORKSPACE_DIR}".'))
    parser.add_argument(
        'input_raster_path', nargs='+', help='Path(s) to rasters to mask.')
    parser.add_argument(
        'mask_raster_path', help='Path to raster mask, only nodata is considered.')
    args = parser.parse_args()
    LOGGER.debug(args.input_raster_path)
    raster_path_list = [
        raster_path for pattern in args.input_raster_path
        for raster_path in glob.glob(pattern)]
    LOGGER.info('processing these files: ' + ', '.join(raster_path_list))
    invalid_rasters = [
        path for path in raster_path_list
        if geoprocessing.get_gis_type(path) != RASTER_TYPE]

    if invalid_rasters:
        raise ValueError(
            f'the following raster(s) were passed in as rasters but are not: '
            f'{",".join(invalid_rasters)}')

    for pattern in args.input_raster_path:
        for raster_path in glob.glob(pattern):
            LOGGER.debug(raster_path)
            target_mask_raster_path = os.path.join(
                WORKSPACE_DIR, f'masked_{os.path.basename(raster_path)}')
            mask_raster(
                raster_path, args.mask_raster_path,
                target_mask_raster_path)
    LOGGER.info('all done!')


if __name__ == '__main__':
    main()
