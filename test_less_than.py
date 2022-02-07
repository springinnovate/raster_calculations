"""These calculations are for the science paper."""
import sys
import os
import logging
import multiprocessing

import raster_calculations_core
from osgeo import gdal
from ecoshard import taskgraph

gdal.SetCacheMax(2**26)

WORKSPACE_DIR = 'less_than_workspace'
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

    raster_calculation_list = [
        {
            'expression': '(x < 0) * x',
            'symbol_to_path_map': {
                'x': r"C:\Users\rpsharp\Documents\output.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "less_than.tif",
            'build_overview': True,
        },
    ]

    for calculation in raster_calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()


if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(WORKSPACE_DIR, NCPUS, 5.0)
    main()
