"""Mask a stack of rasters to a vector."""
import argparse
import glob
import os
import multiprocessing
import logging

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

WORKSPACE_DIR = 'mask_by_vector_workspace'


def mask_raster(base_raster_path, vector_mask_path, target_raster_path):
    """Mask base by vector to target."""
    base_raster_info = geoprocessing.get_raster_info(base_raster_path)
    geoprocessing.new_raster_from_base(
        base_raster_path, target_raster_path,
        base_raster_info['datatype'], [base_raster_info['nodata'][0]])
    geoprocessing.mask_raster(
        (base_raster_path, 1), vector_mask_path,
        target_raster_path)

def main():
    """Entry point."""
    parser = argparse.ArgumentParser(description=(
        'Mask rasters by vector. Results are in a subdirectory called '
        f'"{WORKSPACE_DIR}".'))
    parser.add_argument(
        'input_raster_path', nargs='+', help='Path(s) to rasters to mask.')
    parser.add_argument(
        'mask_vector_path', help='Path to vector mask.')
    args = parser.parse_args()

    if geoprocessing.get_gis_type(args.mask_vector_path) != VECTOR_TYPE:
        raise ValueError(
            f'expected path to vector but got {args.mask_vector_path}')

    task_graph = taskgraph.TaskGraph(
        WORKSPACE_DIR, multiprocessing.cpu_count(), 15.0)

    raster_path_list = [
        raster_path for pattern in args.input_raster_path
        for raster_path in glob.glob(pattern)]

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
            task_graph.add_task(
                func=mask_raster,
                args=(
                    raster_path, args.mask_vector_path,
                    target_mask_raster_path),
                target_path_list=[target_mask_raster_path],
                task_name=f'mask {target_mask_raster_path}')
    task_graph.close()
    task_graph.join()
    LOGGER.info('all done!')


if __name__ == '__main__':
    main()
