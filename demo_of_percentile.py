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


    path = r"C:\Users\Becky\Documents\raster_calculations\realized_pollination_with_overviews_md5_8a780d5962aea32aaa07941bde7d8832.tif"
    percentile_working_dir = r"C:\Users\Becky\Documents\raster_calculations\percentile_working_dir"
    #makes a temporary directory because there's a shitton of rasters to find out the percentiles
    try:
        os.makedirs(percentile_working_dir)
    except OSError:
        pass
        #checks to see if the directory already exists, if it doesn't it makes it, if it does it doesn't do anything
    percentile_values_list = pygeoprocessing.raster_band_percentile(
        (path, 1), percentile_working_dir, [0, 1, 25, 50, 75, 95, 99, 100])
    # (path,1) is indicating the first band in that "path" raster; the 2nd argument is the working dir; the third is the list of percentiles we want
    shutil.rmtree(percentile_working_dir)
    #this gets rid of that termporary directory
    print(percentile_values_list)
    return  # terminates at this point

#realized_nitrogenretention_downstream_md5_82d4e57042482eb1b92d03c0d387f501
#        0th           25th    50th                 75th                   95th               99th                 100th    
#  -680618713.5250311, 0.0, 13816.789725814846, 6,973,767.683561915, 79,365,495.3211188, 301,244,797.7473311, 38,828,470,375.24423
# realized_sedimentdeposition_downstream_md5_1613b12643898c1475c5ec3180836770
#        0th           1st  25th  50th                 75th                   95th               99th                 100th  
# [-2858.336473792897, 0.0, 0.0, 0.0,            402.0619507020006,    357620.664287882,   16,535,749.256560648,   6,597,664,634,840.14]

    percentile_expression = {
        'expression': 'service / percentile(service, 100.0)',
        'symbol_to_path_map': {
            'service': r"C:\Users\rpsharp\Documents\bitbucket_repos\raster_calculations\lspop2017_md5_86d653478c1d99d4c6e271bad280637d.tif",
        },
        'target_nodata': -1,
        'target_raster_path': "normalized_service.tif",
    }
    raster_calculations_core.evaluate_calculation(
        percentile_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    


if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(WORKSPACE_DIR, NCPUS, 5.0)
    main()
