"""These calculations are for the Critical Natural Capital paper."""
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

    single_expression = {
        'expression': 'raster1*raster2',
        'symbol_to_path_map': { 
            'raster1': r"C:\Users\Becky\Documents\unilever\scenarios\September\NEAREST_missingcarbon_biomass_per_ha_PNVESA-ESA2014_forestonly.tif",
            'raster2': r"C:\Users\Becky\Documents\unilever\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_pixel_area_land_mask.tif"
        },
        'target_nodata': float(numpy.finfo(numpy.float32).min),
        'default_nan': float(numpy.finfo(numpy.float32).min),
        'target_pixel_size': (0.00277777780000000021,-0.00277777780000000021),
        'resample_method': 'near',
        'target_raster_path': "NEAREST_missingcarbon_biomass_per_PIXEL_PNVESA-ESA2014_forestonly.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()



if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(WORKSPACE_DIR, NCPUS, 5.0)
    main()

