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

    single_expression = {
        'expression': '(raster2>-9999)*raster1',
        'symbol_to_path_map': {
            'raster1': r"solution_111_tar_80_res_2km_carbon_0.tif",
            'raster2': r"realized_e_source_abs_ann_mean.tif"
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_projection_wkt': wgs84_srs.ExportToWkt(),
        'target_pixel_size': (1.495833333333333348,1.5092592592592593),
        'resample_method': 'average',
        'target_raster_path': "top80_solution_1.5d_avg.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(WORKSPACE_DIR, NCPUS, 5.0)
    main()

