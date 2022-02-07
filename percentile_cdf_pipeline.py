"""
Percentile calculations.

Count non-zero non-nodata pixels in raster, get percentile values, and sum
above each percentile to build the CDF.
"""
import argparse
import pickle
import tempfile
import glob
import sys
import os
import shutil
import logging
import multiprocessing

from ecoshard import taskgraph
from ecoshard import geoprocessing
from osgeo import gdal
import numpy

gdal.SetCacheMax(2**26)

WORKSPACE_DIR = 'raster_calculations'
N_CPUS = max(1, multiprocessing.cpu_count() - 2)
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
    percentile_working_dir = r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\percentile_working_dir"
    try:
        os.makedirs(percentile_working_dir)
    except OSError:
        pass

    table_path = r"C:\Users\Becky\Documents\cnc_project\carbon_percentiles_table.csv"
    # this is the directory the loop will search through
    base_directory = r"C:\Users\Becky\Documents\cnc_project\original_rasters\carbon"
    # you can modify this list and the rest of the code will adapt
    # make a list full of 0s as long as the percentile list
    percentiles_list = list(range(0, 101, 1))
        #[0, 0.01, 1, 2, 3, 4, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 96, 97, 98, 99, 99.9, 100]

    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, N_CPUS, 5.0)

    pickle_path_list = []
    # this will loop through every file that ends in ".tif" in the base
    # directory
    raster_path_list = glob.glob(os.path.join(base_directory, '*.tif'))
    for raster_path in sorted(raster_path_list):
        LOGGER.debug('processing %s', raster_path)
        result_pickle_path = os.path.join(
            percentile_working_dir, '%s.pickle' % (
                os.path.splitext(os.path.basename(raster_path)))[0])
        pickle_path_list.append(result_pickle_path)
        _ = task_graph.add_task(
            func=calculate_percentile,
            args=(
                raster_path, percentiles_list, percentile_working_dir,
                result_pickle_path),
            target_path_list=[result_pickle_path],
            task_name='%s percentile' % raster_path)

    LOGGER.debug('waiting for pipeline to process')
    task_graph.join()
    LOGGER.debug('saving results to a csv table')
    table_file = open(table_path, 'w')
    for result_pickle_path, raster_path in zip(
            pickle_path_list, raster_path_list):
        raster_filename = os.path.basename(raster_path)
        LOGGER.debug('loading: %s', result_pickle_path)
        with open(result_pickle_path, 'rb') as result_pickle_file:
            result_dict = pickle.load(result_pickle_file)
        LOGGER.debug(result_dict)
        table_file.write('%s\n' % raster_filename)
        table_file.write('percentile,percentile_value,percentile_sum\n')
        pixel_stats_string = (
            '\n'.join(['%f,%.10e,%.10e' % (
                percentile, percentile_value, percentile_sum)
                   for percentile, percentile_value, percentile_sum in zip(
                   result_dict['percentiles_list'],
                   result_dict['percentile_values_list'],
                   result_dict['percentile_sum_list'])]))
        table_file.write(pixel_stats_string)
        table_file.write('\n')
    table_file.close()


def calculate_percentile(
        raster_path, percentiles_list, workspace_dir, result_pickle_path):
    """Calculate the percentile cutoffs of a given raster. Store in json.

    Parameters:
        raster_path (str): path to raster to calculate over.
        percentiles_list (list): sorted list of increasing percentile
            cutoffs to calculate.
        workspace_dir (str): path to a directory where this function can
            create a temporary directory to work in.
        result_pickle_path (path): path to .json file that will store
            "percentiles_list" -- original value of perentile_list
            "percentile_values_list" -- list of percentile threshold values
                in the same position in `percentile_list`.
            "percentile_sums_list" -- sum of all values up to the given
                percentile in the same position in `percentile_list`.

    Returns:
        None.

    """
    churn_dir = tempfile.mkdtemp(dir=workspace_dir)
    LOGGER.debug('processing percentiles for %s', raster_path)
    heap_size = 2**28
    ffi_buffer_size = 2**10
    result_dict = {
        'percentiles_list': percentiles_list,
        'percentile_sum_list': [0.] * len(percentiles_list),
        'percentile_values_list': geoprocessing.raster_band_percentile(
            (raster_path, 1), churn_dir, percentiles_list,
            heap_size, ffi_buffer_size)
    }
    LOGGER.debug('intermediate result_dict: %s', str(result_dict))
    LOGGER.debug('processing percentile sums for %s', raster_path)
    nodata_value = geoprocessing.get_raster_info(raster_path)['nodata'][0]
    for _, block_data in geoprocessing.iterblocks((raster_path, 1)):
        nodata_mask = numpy.isclose(block_data, nodata_value)
        # this loop makes the block below a lot simpler
        for index, percentile_value in enumerate(
                result_dict['percentile_values_list']):
            mask = (block_data > percentile_value) & (~nodata_mask)
            result_dict['percentile_sum_list'][index] += (
                numpy.sum(block_data[mask]))

    LOGGER.debug(
        'pickling percentile results of %s to %s', raster_path,
        result_pickle_path)
    with open(result_pickle_path, 'wb') as pickle_file:
        pickle_file.write(pickle.dumps(result_dict))
    shutil.rmtree(churn_dir)


if __name__ == '__main__':
    file_logger = logging.FileHandler('percentile_cdf_log.txt')
    file_logger.setLevel(logging.DEBUG)
    logging.getLogger().addHandler(file_logger)

    parser = argparse.ArgumentParser(description='Run CDF pipeline')
    parser.add_argument(
        '--n_cpus', dest='n_cpus', type=int, default=N_CPUS)
    args = parser.parse_args()
    N_CPUS = args.n_cpus
    main()
