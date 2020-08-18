"""Demo percentile function."""
import sys
import os
import shutil
import logging
import multiprocessing

import pygeoprocessing
import raster_calculations_core
from osgeo import gdal
import taskgraph

gdal.SetCacheMax(2**30)

WORKSPACE_DIR = 'CNC_workspace'
NCPUS = -1
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

    path = r"C:\Users\Becky\Documents\IPBES off dropbox\lspop2017.tif"
    percentile_working_dir = r"C:\Users\Becky\Documents\raster_calculations\percentile_working_dir"
    #makes a temporary directory because there's a shitton of rasters to find out the percentiles
    try:
        os.makedirs(percentile_working_dir)
    except OSError:
        pass
        #checks to see if the directory already exists, if it doesn't it makes it, if it does it doesn't do anything
    percentile_values_list = pygeoprocessing.raster_band_percentile(
        (path, 1), percentile_working_dir, list(range(0, 101, 1)))
    # (path,1) is indicating the first band in that "path" raster; the 2nd argument is the working dir; the third is the list of percentiles we want
    shutil.rmtree(percentile_working_dir)
    #this gets rid of that termporary directory
    print(percentile_values_list)

   




if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(WORKSPACE_DIR, NCPUS, 5.0)
    main()
