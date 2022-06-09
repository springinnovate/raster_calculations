"""Demo percentile function."""
import sys
import os
import shutil
import logging

from ecoshard import geoprocessing
from osgeo import gdal
from ecoshard import taskgraph

gdal.SetCacheMax(2**26)

WORKSPACE_DIR = 'poll_workspace'
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

    #path = r"D:\results\change\ndv_0.0_sed_export_marineESA_2020-1992_change_md5_0ab0cf.tif"
    #path = r"D:\results\change\ndv_0.0_sed_deposition_marineESA_2020-1992_change_md5_d23c49.tif"
    path = r"D:\results\change\ndv_0.0_realized_pollination_on_hab_marESA_2020-1992_fullchange_md5_e39bad.tif"
    #path = r"D:\results\change\ndv_0.0_realized_pollination_on_ag_marESA_2020-1992_fullchange_md5_8e63e2.tif"
    #path = r"D:\results\change\ndv_0.0_nature_access_lspop2019_marESA2020-1992_change_md5_c324c9.tif"
    #path = r"D:\results\change\ndv_0.0_n_export_marineESA_2020-1992_change_val_md5_18a2b3.tif"
    #path = r"D:\results\change\ndv_0.0_cv_habitat_value_marESA2020-1992_change_md5_1643a7.tif"

    percentile_working_dir = r"D:\repositories\raster_calculations\poll_working_dir"
    #makes a temporary directory because there's a shitton of rasters to find out the percentiles
    try:
        os.makedirs(percentile_working_dir)
    except OSError:
        pass
        #checks to see if the directory already exists, if it doesn't it makes it, if it does it doesn't do anything
    percentile_values_list = geoprocessing.raster_band_percentile(
        #(path, 1), percentile_working_dir, [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 99, 99.5, 99.9, 99.99, 99.999, 100] )
        (path, 1), percentile_working_dir, list(range(0, 101, 1)))
    # (path,1) is indicating the first band in that "path" raster; the 2nd argument is the working dir; the third is the list of percentiles we want
    shutil.rmtree(percentile_working_dir)
    #this gets rid of that termporary directory
    print(percentile_values_list)






if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(WORKSPACE_DIR, NCPUS, 5.0)
    main()
