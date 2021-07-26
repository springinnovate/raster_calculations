"""Resample a mask with custom rules."""
import argparse
import logging
import math
import os
import shutil
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
    parser.add_argument(
        '--reproject_file', nargs=6, type=str, help=(
            '6 arguments: {path to projection wkt} '
            '{pixel size in projected units} '
            'bounding box {xmin}, {ymin} {xmax} {ymax}'))
    args = parser.parse_args()

    task_graph = taskgraph.TaskGraph('.', -1)
    WORKSPACE_DIR = tempfile.mkdtemp(dir='.', prefix='resample_workspace')
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
        kwargs={'mask_nodata': False},
        target_path_list=[sampled_raster_path],
        task_name='convolve_2d')

    threshold_raster_path = os.path.join(WORKSPACE_DIR, 'threshold.tif')
    task_graph.add_task(
        func=ecoshard.geoprocessing.raster_calculator,
        args=(
            [(sampled_raster_path, 1), (args.pixel_proportion_to_mask, 'raw')],
            threshold_mask, threshold_raster_path,
            gdal.GDT_Byte, None),
        target_path_list=[threshold_raster_path],
        task_name='threshold')

    task_graph.add_task(
        func=ecoshard.geoprocessing.warp_raster,
        args=(
            threshold_raster_path,
            (args.target_cell_size, -args.target_cell_size),
            args.target_raster_path, 'near'),
        target_path_list=[args.target_raster_path],
        task_name=f'warp to {args.target_cell_size}')

    task_graph.join()
    if args.reproject_file:
        LOGGER.info(f'reproject to {args.reproject_file}')
        unprojected_path = os.path.join(
            WORKSPACE_DIR, os.path.basename(args.target_raster_path))
        shutil.copy(args.target_raster_path, unprojected_path)
        with open(args.reproject_file[0], 'r') as reproject_file:
            target_projection_wkt = reproject_file.read()
        # target pixel size is specified by the user
        target_pixel_size = [
            float(args.reproject_file[1]), -float(args.reproject_file[1])]

        target_bounding_box = [float(x) for x in args.reproject_file[2:]]
        LOGGER.debug(
            f'warp raster to new projection with bounding box '
            f'{target_bounding_box}')
        ecoshard.geoprocessing.warp_raster(
            unprojected_path, target_pixel_size, args.target_raster_path,
            'near', target_projection_wkt=target_projection_wkt,
            target_bb=target_bounding_box)
    shutil.rmtree(WORKSPACE_DIR)


if __name__ == '__main__':
    main()
