"""Script to stitch arbitrary rasters together."""
import argparse
import glob
import logging
import multiprocessing
import os
import tempfile

from osgeo import gdal
from osgeo import osr
import pygeoprocessing
import taskgraph


logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(__name__)

gdal.SetCacheMax(2**30)

if __name__ == '__main__':
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
        'raster_list', nargs='+',
        help='List of rasters or wildcards')

    args = parser.parse_args()

    raster_path_list = [
        raster_path for raster_glob in args.raster_list
        for raster_path in glob.glob(raster_glob)
        ]

    print(raster_path_list)

    target_projection = osr.SpatialReference()
    target_projection.ImportFromEPSG(int(args.target_projection_epsg))

    temp_working_dir = tempfile.mkdtemp(dir='.')

    task_graph = taskgraph.TaskGraph(temp_working_dir, n_workers=multiprocessing.cpu_count())
    target_bounding_box_list = []
    reprojected_raster_path_task_list = []
    for raster_path in raster_path_list:
        raster_info = pygeoprocessing.get_raster_info(raster_path)
        bounding_box = raster_info['bounding_box']
        target_bounding_box = pygeoprocessing.transform_bounding_box(
            bounding_box, raster_info['projection_wkt'],
            target_projection.ExportToWkt())
        target_bounding_box_list.append(target_bounding_box)
        reprojected_raster_path = os.path.join(
            temp_working_dir, os.path.basename(raster_path))
        cell_size = (
            float(args.target_cell_size), -float(args.target_cell_size))
        warp_task = task_graph.add_task(
            func=pygeoprocessing.warp_raster,
            args=(
                raster_path, cell_size,
                reprojected_raster_path, args.resample_method),
            kwargs={
                'target_projection_wkt': target_projection.ExportToWkt(),
                'working_dir': temp_working_dir,
            },
            target_path_list=[reprojected_raster_path],
            task_name=f'reproject {os.path.basename(reprojected_raster_path)}')
        reprojected_raster_path_task_list.append(
            (reprojected_raster_path, warp_task))

    target_bounding_box = pygeoprocessing.merge_bounding_box_list(
        target_bounding_box_list, 'union')

    gtiff_driver = gdal.GetDriverByName('GTiff')

    n_cols = int(
        (target_bounding_box[2]-target_bounding_box[0]) / cell_size[0])
    n_rows = int(
        (target_bounding_box[3]-target_bounding_box[1]) / -cell_size[1])

    geotransform = (
        target_bounding_box[0], cell_size[0], 0.0,
        target_bounding_box[3], 0.0, cell_size[1])

    target_raster = gtiff_driver.Create(
        args.target_raster_path, n_cols, n_rows, 1, raster_info['datatype'],
        options=(
            'TILED=YES', 'BIGTIFF=YES', 'BLOCKXSIZE=256', 'BLOCKYSIZE=256',
            'COMPRESS=LZW', 'SPARSE_OK=TRUE'))
    target_raster.SetProjection(target_projection.ExportToWkt())
    target_raster.SetGeoTransform(geotransform)
    target_band = target_raster.GetRasterBand(1)
    target_band.SetNoDataValue(raster_info['nodata'][0])
    target_band = None
    target_raster = None

    target_raster = gdal.OpenEx(
        args.target_raster_path, gdal.OF_RASTER | gdal.GA_Update)
    target_band = target_raster.GetRasterBand(1)

    task_graph.close()

    for reprojected_raster, warp_task in reprojected_raster_path_task_list:
        warp_task.join()
        reprojected_info = pygeoprocessing.get_raster_info(reprojected_raster)
        reprojected_raster = gdal.OpenEx(reprojected_raster, gdal.OF_RASTER)
        reprojected_array = reprojected_raster.ReadAsArray()
        reprojected_raster = None
        xpos = int(
            (target_bounding_box[0] - reprojected_info['bounding_box'][0]) /
            cell_size[0])
        ypos = int(
            (target_bounding_box[2] - reprojected_info['bounding_box'][2]) /
            -cell_size[1])

        try:
            target_band.WriteArray(xpos, ypos, reprojected_array)
        except Exception:
            LOGGER.exception(
                f"this array couldn't 'stitch: {reprojected_array}")

    task_graph.join()
