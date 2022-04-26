"""These calculations are for the CI global restoration project."""
#conda activate py38_gdal312
#conda activate py39_ecoprocess

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
#import taskgraph
#import pygeoprocessing
from ecoshard import taskgraph
import ecoshard.geoprocessing as pygeoprocessing

gdal.SetCacheMax(2**25)

WORKSPACE_DIR = 'CNC_workspace'
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

    calculation_list = [
        {
            'expression': '(raster1>0)*raster1 + (raster1<0)*-9999',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_n_export_marine_mod_ESA_2020_fertilizer_current_compressed_md5_8ba187.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "global_n_export_ESAmar_2020_fertilizer_current_valid.tif",
        },
        {
            'expression': '(raster1>0)*raster1 + (raster1<0)*-9999',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_n_export_marine_mod_ESA_1992_fertilizer_current_compressed_md5_b1bb72.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "global_n_export_ESAmar_1992_fertilizer_current_valid.tif",
        },

    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_n_export_marine_mod_ESA_2020_fertilizer_current_compressed_md5_8ba187.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_n_export_marine_mod_ESA_1992_fertilizer_current_compressed_md5_b1bb72.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "n_export_marineESA_2020-1992_change.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_sed_export_marine_mod_ESA_2020_compressed_md5_a988c0.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_sed_export_marine_mod_ESA_1992_compressed_md5_18eaae.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "sed_export_marineESA_2020-1992_change.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(WORKSPACE_DIR, NCPUS, 5.0)
    main()
