"""These calculations are just for fun."""
# don't forget: conda activate py38_gdal312

import glob
import sys
import os
import logging
import multiprocessing
import datetime
import subprocess
import raster_calculations_core
from osgeo import gdal
from osgeo import osr
import taskgraph
import pygeoprocessing
import numpy

gdal.SetCacheMax(2**30)

WORKSPACE_DIR = 'rastercalc_workspace'
NCPUS = multiprocessing.cpu_count()
try:
    os.makedirs(WORKSPACE_DIR)
except OSError:
    pass

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


def main():
    """Write your expression here."""

    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)

    sinusoidal_raster1_path = 'solution_111_tar_80_res_2km_carbon_0.tif'
    raster_2_path = 'realized_e_source_abs_ann_mean.tif'
    raster_3_path = '1.5d_ha_per_pixel.tif'
    raster2_info = pygeoprocessing.get_raster_info(raster_2_path)
    target_pixel_size = (1.495833333333333348, 1.5092592592592593)
    resample_method = 'average'
    projected_raster1_path = f'wgs84_projected_{sinusoidal_raster1_path}'

    warp_raster_task = TASK_GRAPH.add_task(
        func=pygeoprocessing.warp_raster,
        args=(
            sinusoidal_raster1_path, target_pixel_size, projected_raster1_path,
            resample_method),
        kwargs={
            'target_projection_wkt': wgs84_srs.ExportToWkt(),
            'target_bb': raster2_info['bounding_box']},
        target_path_list=[projected_raster1_path],
        task_name=f'project {sinusoidal_raster1_path}')
    warp_raster_task.join()

    single_expression = {
        'expression': '(raster2>-9999)*raster1*raster3',
        'symbol_to_path_map': {
            'raster1': projected_raster1_path,
            'raster2': raster_2_path,
            'raster3': raster_3_path,
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_projection_wkt': wgs84_srs.ExportToWkt(),
        'target_pixel_size': target_pixel_size,
        'resample_method': resample_method,
        'target_raster_path': "top80_solution_area_1.5d.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(WORKSPACE_DIR, NCPUS, 5.0)
    main()

