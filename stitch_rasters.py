"""Script to stitch arbitrary rasters together."""
import argparse
import glob
import itertools
import logging
import math
import multiprocessing
import os

from osgeo import gdal
from osgeo import osr
import pygeoprocessing
import numpy
import taskgraph


logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.DEBUG)
gdal.SetCacheMax(2**26)


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(description='Ecoshard files.')
    parser.add_argument(
        '--target_projection_epsg', required=True,
        help='EPSG code of target projection')
    parser.add_argument(
        '--target_cell_size', required=True,
        help=(
            'A single float indicating the desired square pixel size of '
            'the stitched raster.'))
    parser.add_argument(
        '--resample_method', default='near', help=(
            'One of near|bilinear|cubic|cubicspline|lanczos|average|mode|max'
            'min|med|q1|q3'))
    parser.add_argument(
        '--target_raster_path', required=True, help='Path to target raster.')
    parser.add_argument(
        '--raster_list', nargs='+',
        help='List of rasters or wildcards to stitch.')
    parser.add_argument(
        '--raster_pattern', nargs=2, help=(
            'Recursive directory search for raster pattern such that '
            'the first argument is the directory to search and the second '
            'is the filename pattern.'))
    parser.add_argument(
        '--_n_limit', type=int,
        help=(
            'limit the number of stitches to this number, default is to '
            'stitch all found rasters'))

    args = parser.parse_args()

    if not args.raster_list != args.raster_pattern:
        raise ValueError(
            'only one of --raster_list or --raster_pattern must be '
            'specified: \n'
            f'args.raster_list={args.raster_list}\n'
            f'args.raster_pattern={args.raster_pattern}\n')

    if args.raster_list:
        raster_path_list = (
            raster_path for raster_glob in args.raster_list
            for raster_path in glob.glob(raster_glob)
            )
    else:
        base_dir = args.raster_pattern[0]
        file_pattern = args.raster_pattern[1]

        raster_path_list = itertools.islice(
            (raster_path for walk_info in os.walk(base_dir)
             for raster_path in glob.glob(os.path.join(
                walk_info[0], file_pattern))), 0, args._n_limit)

    target_projection = osr.SpatialReference()
    target_projection.ImportFromEPSG(int(args.target_projection_epsg))

    working_dir = (
        'working_dir_'
        f'{os.path.basename(os.path.splitext(args.target_raster_path)[0])}')

    task_graph = taskgraph.TaskGraph(
        working_dir, n_workers=0) #multiprocessing.cpu_count())
    target_bounding_box_list = []
    reprojected_raster_path_task_list = []
    raster_path_set = set()
    for raster_path in raster_path_list:
        LOGGER.debug(f'stitch {raster_path}')
        if raster_path in raster_path_set:
            LOGGER.warning(f'{raster_path} already scheduled')
            continue
        LOGGER.debug(f'schedule {raster_path}')
        raster_path_set.add(raster_path)
        raster_info = pygeoprocessing.get_raster_info(raster_path)
        bounding_box = raster_info['bounding_box']
        target_bounding_box = pygeoprocessing.transform_bounding_box(
            bounding_box, raster_info['projection_wkt'],
            target_projection.ExportToWkt())
        target_bounding_box_list.append(target_bounding_box)
        reprojected_raster_path = os.path.join(
            working_dir, os.path.basename(raster_path))
        cell_size = (
            float(args.target_cell_size), -float(args.target_cell_size))
        warp_task = task_graph.add_task(
            func=pygeoprocessing.warp_raster,
            args=(
                raster_path, cell_size,
                reprojected_raster_path, args.resample_method),
            kwargs={
                'target_projection_wkt': target_projection.ExportToWkt(),
                'working_dir': working_dir,
            },
            target_path_list=[reprojected_raster_path],
            task_name=f'reproject {os.path.basename(reprojected_raster_path)}')
        reprojected_raster_path_task_list.append(
            (reprojected_raster_path, warp_task))

    target_bounding_box = pygeoprocessing.merge_bounding_box_list(
        target_bounding_box_list, 'union')

    gtiff_driver = gdal.GetDriverByName('GTiff')

    n_cols = int(math.ceil(
        (target_bounding_box[2]-target_bounding_box[0]) / cell_size[0]))
    n_rows = int(math.ceil(
        (target_bounding_box[3]-target_bounding_box[1]) / -cell_size[1]))

    geotransform = (
        target_bounding_box[0], cell_size[0], 0.0,
        target_bounding_box[3], 0.0, cell_size[1])

    target_raster = gtiff_driver.Create(
        os.path.join('.', args.target_raster_path), n_cols, n_rows, 1,
        raster_info['datatype'],
        options=(
            'TILED=YES', 'BIGTIFF=YES', 'BLOCKXSIZE=256', 'BLOCKYSIZE=256',
            'COMPRESS=LZW', 'SPARSE_OK=TRUE'))
    target_raster.SetProjection(target_projection.ExportToWkt())
    target_raster.SetGeoTransform(geotransform)
    target_band = target_raster.GetRasterBand(1)
    target_band.SetNoDataValue(raster_info['nodata'][0])

    for reprojected_raster_path, warp_task in \
            reprojected_raster_path_task_list:
        LOGGER.info(f'joining task for {reprojected_raster_path}')
        warp_task.join()

        _stitch_into(
            reprojected_raster_path, cell_size,
            target_bounding_box[0],
            target_bounding_box[3], target_band)

    target_band = None
    target_raster = None

    LOGGER.info('closing taskgraph')
    task_graph.close()
    task_graph.join()
    task_graph._terminate()
    LOGGER.info('all done')


def _stitch_into(
        raster_path, cell_size, target_x_origin, target_y_origin,
        target_band):
    """Stitch raster path into target.

    Args:
        raster_path (str): path to raster to stitch into target
        target_band (Band): path to existing raster to stitch into.

    Returns:
        None.
    """
    raster_info = pygeoprocessing.get_raster_info(raster_path)
    nodata = raster_info['nodata'][0]

    x_origin = int(
        (raster_info['bounding_box'][0] - target_x_origin) / cell_size[0])
    y_origin = int(
        (raster_info['bounding_box'][3] - target_y_origin) / cell_size[1])
    LOGGER.debug(f'stitch {raster_path} to {x_origin} {y_origin}')

    try:
        for offset_dict, block_array in pygeoprocessing.iterblocks(
                (raster_path, 1)):
            global_xoff = x_origin+offset_dict['xoff']
            global_yoff = y_origin+offset_dict['yoff']
            target_array = target_band.ReadAsArray(
                xoff=global_xoff,
                yoff=global_yoff,
                win_xsize=offset_dict['win_xsize'],
                win_ysize=offset_dict['win_ysize'],
                )
            target_band.WriteArray(
                numpy.where(numpy.isclose(
                    block_array, nodata), target_array, block_array),
                xoff=x_origin+offset_dict['xoff'],
                yoff=y_origin+offset_dict['yoff'])

    except Exception:
        LOGGER.exception(
            f"this array couldn't 'stitch: {raster_path}")


if __name__ == '__main__':
    main()
