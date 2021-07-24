"""Resample a mask with custom rules."""
import argparse
import logging
import math
import os
import sys
import tempfile

from osgeo import gdal
from osgeo import osr
import ecoshard.geoprocessing
import numpy
import taskgraph

gdal.SetCacheMax(2**27)

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.WARN)


def threshold_mask(prop_array, threshold):
    result = numpy.zeros(prop_array.shape, dtype=bool)
    result[prop_array >= threshold] = 1
    return result


def main():
    """Entry point."""
    LOGGER.info('hello')
    parser = argparse.ArgumentParser(description='Resample mask')
    parser.add_argument(
        'mask_raster_path',
        help='path to mask raster')
    parser.add_argument(
        'target_cell_size',
        type=float, help='new cell size in raster units')
    parser.add_argument(
        'pixel_proportion_to_mask',
        type=float, help='what percent of pixels need to be masked to turn on')
    parser.add_argument(
        'target_raster_path', type=str,
        help='path to resampled target raster')
    args = parser.parse_args()

    task_graph = taskgraph.TaskGraph('.', -1)
    WORKSPACE_DIR = 'resample_workspace' #tempfile.mkdtmp(dir='.', prefix='resample_workspace')
    os.makedirs(WORKSPACE_DIR, exist_ok=True)

    mask_info = ecoshard.geoprocessing.get_raster_info(args.mask_raster_path)

    # calculate new raster size
    n_cols, n_rows = mask_info['raster_size']

    change_ratio = args.target_cell_size / mask_info['pixel_size'][0]
    LOGGER.info(f'raster size is scaled by {change_ratio}')
    LOGGER.info(
        f"original raster is {mask_info['pixel_size']}, "
        f'new raster is {n_cols/change_ratio}, {n_rows/change_ratio}')

    ones_array = numpy.ones(
        (int(math.ceil(change_ratio)), int(math.ceil(change_ratio))),
        dtype=numpy.float32)
    ones_array[:] /= ones_array.size
    kernel_raster_path = os.path.join(WORKSPACE_DIR, 'kernel.tif')
    task_graph.add_task(
        func=ecoshard.geoprocessing.numpy_array_to_raster,
        args=(
            ones_array, None, (1, -1), (0, 0), osr.SRS_WKT_WGS84_LAT_LONG,
            kernel_raster_path),
        target_path_list=[kernel_raster_path])

    sampled_raster_path = os.path.join(WORKSPACE_DIR, 'sampled.tif')
    task_graph.add_task(
        func=ecoshard.geoprocessing.convolve_2d,
        args=(
            (args.mask_raster_path, 1),
            (kernel_raster_path, 1), sampled_raster_path),
        target_path_list=[sampled_raster_path],
        task_name='convolve_2d')

    task_graph.add_task(
        func=ecoshard.geoprocessing.raster_calculator,
        args=(
            [(sampled_raster_path, 1), (args.pixel_proportion_to_mask, 'raw')],
            threshold_mask, args.target_raster_path,
            gdal.GDT_Byte, None),
        target_path_list=[args.target_raster_path],
        task_name='threshold')


if __name__ == '__main__':
    main()
