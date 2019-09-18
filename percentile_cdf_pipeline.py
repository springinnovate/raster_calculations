"""Count non-zero non-nodata pixels in raster, get percentile values, and sum above each percentile to build the CDF."""
import glob
import sys
import os
import shutil
import logging
import multiprocessing

import pygeoprocessing
import numpy
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

    # commenting out because you have a loop to do this now
    #path = r"C:\Users\Becky\Documents\raster_calculations\aggregate_potential_ES_score_nspwng.tif"
    ## How would I loop through a set of paths -- do this for a whole directory of rasters, and have it print out (or even better, add to a table) as it completes each one?
    percentile_working_dir = r"C:\Users\Becky\Documents\raster_calculations\percentile_working_dir"
    try:
        os.makedirs(percentile_working_dir)
    except OSError:
        pass

    # this is the directory the loop will search through
    base_directory = r"C:\Users\Becky\Documents\raster_calculations"
    # this will loop through every file that ends in ".tif" in the base
    # directory
    for path in glob.glob(os.path.join(base_directory, '*.tif')):
        # inside the loop it's gonna process just *one* file so this might
        # be a great place to write results to a table instead of printing

        # you can modify this list and the rest of the code will adapt
        percentiles_list = [
            0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 97.5, 100]
        # make a list full of 0s as long as the percentile list
        percentile_sum_list = [0.] * len(percentiles_list)
        percentile_values_list = pygeoprocessing.raster_band_percentile(
            (path, 1), percentile_working_dir, percentiles_list)
        shutil.rmtree(percentile_working_dir)

        print(path, percentile_values_list)

        nodata_value = pygeoprocessing.get_raster_info(path)['nodata'][0]
        top2_sum = 0.0
        top5_sum = 0.0
        top10_sum = 0.0
        top20_sum = 0.0
        top30_sum = 0.0
        top40_sum = 0.0
        top50_sum = 0.0
        top60_sum = 0.0
        top70_sum = 0.0
        top80_sum = 0.0
        top90_sum = 0.0
        full_sum = 0.0

        for _, block_data in pygeoprocessing.iterblocks((path, 1)):
            nodata_mask = numpy.isclose(block_data, nodata_value)
            # this loop makes the block below a lot simpler
            for index, percentile in enumerate(percentiles_list):
                mask = (block_data > percentile) & (~nodata_mask)
                percentile_sum_list[index] += numpy.sum(block_data[mask])

            # top2_mask = block_data > percentile_values_list[12]
            # top2_sum += numpy.sum(block_data[top2_mask & ~nodata_mask])
            # top5_mask = block_data > percentile_values_list[11]
            # top5_sum += numpy.sum(block_data[top5_mask & ~nodata_mask])
            # top10_mask = block_data > percentile_values_list[10]
            # top10_sum += numpy.sum(block_data[top10_mask & ~nodata_mask])
            # top20_mask = block_data > percentile_values_list[9]
            # top20_sum += numpy.sum(block_data[top20_mask & ~nodata_mask])
            # top30_mask = block_data > percentile_values_list[8]
            # top30_sum += numpy.sum(block_data[top30_mask & ~nodata_mask])
            # top40_mask = block_data > percentile_values_list[7]
            # top40_sum += numpy.sum(block_data[top40_mask & ~nodata_mask])
            # top50_mask = block_data > percentile_values_list[6]
            # top50_sum += numpy.sum(block_data[top50_mask & ~nodata_mask])
            # top60_mask = block_data > percentile_values_list[5]
            # top60_sum += numpy.sum(block_data[top60_mask & ~nodata_mask])
            # top70_mask = block_data > percentile_values_list[4]
            # top70_sum += numpy.sum(block_data[top70_mask & ~nodata_mask])
            # top80_mask = block_data > percentile_values_list[3]
            # top80_sum += numpy.sum(block_data[top80_mask & ~nodata_mask])
            # top90_mask = block_data > percentile_values_list[2]
            # top90_sum += numpy.sum(block_data[top90_mask & ~nodata_mask])

            nonzero_mask = block_data != 0
            full_sum += numpy.sum(block_data[nonzero_mask & ~nodata_mask])

        # this is a fancy way of making a list of strings from each of the
        # pairs of percentiles and their sums such that the percentile is
        # listed first and the sum last. The '\n'.join(..) on the front then
        # turns a list of strings into a single string joined by '\n' so
        # '\n'.join(['good', 'one']) turns into 'good\none', you can then
        # stick that whole thing in a print statement.
        pixel_stats_string = (
            '\n'.join(['%5.1f %14.2f' % (percentile, percentile_sum)
                       for percentile, percentile_sum in zip(
                       percentiles_list, percentile_sum_list)]))
        print('Pixel sum stats from %s\n%s\n' % (path, pixel_stats_string))

        # print(
        #     'Pixel sum stats from %s\n'
        #     '2.5                    %14.2f\n'
        #     '5                      %14.2f\n'
        #     '10                     %14.2f\n'
        #     '20                     %14.2f\n'
        #     '30                     %14.2f\n'
        #     '40                     %14.2f\n'
        #     '50                     %14.2f\n'
        #     '60                     %14.2f\n'
        #     '70                     %14.2f\n'
        #     '80                     %14.2f\n'
        #     '90                     %14.2f\n'
        #     '100                    %14.2f\n' % (
        #         path, top2_sum, top5_sum, top10_sum, top20_sum, top30_sum, top40_sum, top50_sum, top60_sum, top70_sum, top80_sum, top90_sum, full_sum))
