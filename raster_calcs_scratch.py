"""These calculations are for sharing with others."""

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
from ecoshard import taskgraph
import ecoshard.geoprocessing as pygeoprocessing

gdal.SetCacheMax(2**25)

WORKSPACE_DIR = 'scratch_workspace'
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
            'expression': 'raster1-raster2',
            'symbol_to_path_map': {
                'raster1': "D:\repositories\carbon_edge_model\c_stack_hansen_forest_cover2014_compressed_full_forest_edge_result.tif",
                'raster2': "D:\repositories\carbon_edge_model\c_stack_hansen_forest_cover2014_compressed_std_forest_edge_result.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "fc_hansen_2014_missing_carbon.tif",
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
