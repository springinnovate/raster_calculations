"""Count non-zero non-nodata pixels in raster, get percentile values, and sum above each percentile to build the CDF."""
import ecoshard
import itertools
import logging
import logging.handlers
import multiprocessing
import os
import pickle
import sqlite3
import subprocess
import sys

import matplotlib.pyplot
import numpy
import scipy.interpolate
import pygeoprocessing
from osgeo import gdal
from osgeo import osr
from osgeo import ogr
from taskgraph.Task import _execute_sqlite
import taskgraph

gdal.SetCacheMax(2**30)

BUCKET_PATTERN = 'gs://shared-with-users/realized_services/*.tif'
WORKSPACE_DIR = 'cdf_by_country'
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshard')
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')
COUNTRY_WORKSPACES = os.path.join(WORKSPACE_DIR, 'country_workspaces')
NCPUS = -1

logging.basicConfig(
        level=logging.DEBUG,
        format=(
            '%(asctime)s (%(relativeCreated)d) %(processName)s %(levelname)s '
            '%(name)s [%(funcName)s:%(lineno)d] %(message)s'),
        stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
logging.getLogger('matplotlib').setLevel(logging.ERROR)
logging.getLogger('taskgraph').setLevel(logging.INFO)

WORLD_BORDERS_URL = (
    'https://storage.googleapis.com/critical-natural-capital-ecoshards/'
    'countries_iso3_md5_6fb2431e911401992e6e56ddf0a9bcda.gpkg')
COUNTRY_ID_FIELDNAME = 'iso3'

PERCENTILE_LIST = list(range(0, 101, 5))
PERCENTILE_RECLASS_LIST = [
    i/(len(PERCENTILE_LIST)-1) * 10
    for i in range(len(PERCENTILE_LIST))]
BIN_NODATA = -1
WORK_DATABASE_PATH = os.path.join(CHURN_DIR, 'work_status.db')


def country_nodata0_op(base_array, nodata):
    """Convert base_array 0s to nodata."""
    result = base_array.copy()
    result[base_array == 0] = nodata
    return result


def create_status_database(
        database_path, raster_id_list, country_id_list):
    """Create a runtime status database if it doesn't exist.

    Parameters:
        database_path (str): path to database to create.
        raster_id_list (list): list of raster id strings that will be monitored
            for completion.
        country_id_list (list): list of country ids to pair with the rasters.

    Returns:
        None.

    """
    LOGGER.debug('launching create_status_database')
    create_database_sql = (
        """
        CREATE TABLE job_status (
            raster_id TEXT NOT NULL,
            country_id TEXT,
            is_country INT NOT NULL,
            percentile_list BLOB,
            percentile0_list BLOB,
            histogram BLOB,
            histogram0 BLOB,
            path_to_percentile_raster TEXT,
            path_to_percentile0_raster TEXT,
            path_to_histogram_raster TEXT,
            path_to_histogram0_raster);
        """)
    if os.path.exists(database_path):
        os.remove(database_path)
    connection = sqlite3.connect(database_path)
    connection.executescript(create_database_sql)

    # insert countries
    connection.executemany(
        'INSERT INTO job_status(raster_id, country_id, is_country) '
        'VALUES (?, ?, 1)',
        itertools.product(raster_id_list, country_id_list))
    # insert global
    connection.executemany(
        'INSERT INTO job_status(raster_id, is_country) VALUES (?, 0)',
        [(x,) for x in raster_id_list])
    connection.commit()
    connection.close()


def process_country_worker(
        work_queue, world_border_vector_path, raster_id_to_path_map,
        stitch_queue):
    """Process work queue.

    Parameters:
        work_queue (queue): expect ('raster_id', 'country_id') tuples.
            'country_id' can be None which means do the whole raster.
            If None, shut down.
        world_border_vector_path (str): path to a world border vector file
            whose features can be indexed by `country_id`.
        raster_id_to_path_map (dict): maps 'raster_id' to paths to rasters on
            disk.
        stitch_queue (queue): when a country tuple is complete push a
            (bin_path, raster_id, nodata0) tuple down it.

    """
    LOGGER.debug('starting process_country_worker')
    task_graph = taskgraph.TaskGraph(CHURN_DIR, -1)
    while True:
        payload = work_queue.get()
        if payload == 'STOP':
            work_queue.put('STOP')
            LOGGER.debug('stopping')
            break
        raster_id, country_id = payload
        if raster_id not in raster_id_to_path_map:
            continue
        LOGGER.debug('got %s:%s', raster_id, country_id)
        worker_dir = os.path.join(
            COUNTRY_WORKSPACES, '%s_%s' % (raster_id, country_id))
        try:
            os.makedirs(worker_dir)
        except OSError:
            pass

        if country_id:
            country_vector_path = os.path.join(
                worker_dir, '%s.gpkg' % country_id)
            LOGGER.debug('making country vector %s', country_vector_path)
            base_raster_info = pygeoprocessing.get_raster_info(
                raster_id_to_path_map[raster_id])
            country_raster_path = '%s.tif' % os.path.splitext(
                country_vector_path)[0]

            extract_feature_checked_task = task_graph.add_task(
                func=extract_feature_checked,
                args=(
                    world_border_vector_path, 'iso3', country_id,
                    raster_id_to_path_map[raster_id],
                    country_vector_path, country_raster_path),
                target_path_list=[country_raster_path],
                task_name='extract vector %s' % country_id)

            if not extract_feature_checked_task.get():
                with open(os.path.join(worker_dir, 'error.txt')) as error_file:
                    error_file.write(
                        'extraction not work %s:%s',
                        country_id, base_raster_info)
                continue
        else:
            country_raster_path = raster_id_to_path_map[raster_id]

        working_sort_directory = os.path.join(worker_dir, 'percentile_reg')
        percentile_task = task_graph.add_task(
            func=pygeoprocessing.raster_band_percentile,
            args=(
                (country_raster_path, 1), working_sort_directory,
                PERCENTILE_LIST),
            task_name='percentile for %s' % working_sort_directory)
        LOGGER.debug('percentile_task: %s', percentile_task.get())

        country_nodata0_raster_path = '%s_nodata0.tif' % os.path.splitext(
            country_raster_path)[0]
        country_raster_info = pygeoprocessing.get_raster_info(
            country_raster_path)
        country_nodata = country_raster_info['nodata'][0]
        nodata0_raster_task = task_graph.add_task(
            func=pygeoprocessing.raster_calculator,
            args=(
                [(country_raster_path, 1), (country_nodata, 'raw')],
                country_nodata0_op, country_nodata0_raster_path,
                country_raster_info['datatype'], country_nodata),
            target_path_list=[country_nodata0_raster_path],
            task_name='set zero to nodata for %s' % country_raster_path)

        working_sort_nodata0_directory = os.path.join(
            worker_dir, 'percentile_nodata0')
        percentile_nodata0_task = task_graph.add_task(
            func=pygeoprocessing.raster_band_percentile,
            args=(
                (country_nodata0_raster_path, 1),
                working_sort_nodata0_directory, PERCENTILE_LIST),
            dependent_task_list=[nodata0_raster_task],
            task_name='percentile for %s' % working_sort_directory)

        _execute_sqlite(
            '''
                UPDATE job_status
                SET percentile_list=?, percentile0_list=?
                WHERE raster_id=? and country_id=?
            ''',
            WORK_DATABASE_PATH, execute='execute', mode='modify',
            argument_list=[
                pickle.dumps(percentile_task.get()),
                pickle.dumps(percentile_nodata0_task.get()),
                raster_id, country_id])

        LOGGER.debug(
            'percentile_nodata0_task: %s', percentile_nodata0_task.get())
        if country_id:
            bin_raster_path = os.path.join(worker_dir, 'bin_raster.tif')
        else:
            # it's global
            bin_raster_path = os.path.join(
                WORKSPACE_DIR, '%s_bin_raster.tif' % raster_id)
        pygeoprocessing.raster_calculator(
            [(country_raster_path, 1), (country_nodata, 'raw'),
             (percentile_task.get(), 'raw'), (PERCENTILE_RECLASS_LIST, 'raw'),
             (BIN_NODATA, 'raw')], bin_raster_op, bin_raster_path,
            gdal.GDT_Float32, BIN_NODATA)
        stitch_queue.put((bin_raster_path, raster_id, ''))

        if country_id:
            bin_nodata0_raster_path = os.path.join(
                worker_dir, 'bin_nodata0_raster.tif')
        else:
            # it's global
            bin_nodata0_raster_path = os.path.join(
                WORKSPACE_DIR, '%s_bin_nodata0_raster.tif' % raster_id)

        # the first argument is supposed to be `country_raster_path` because
        # we want to leave the 0s in there even though the percentiles are
        # different
        pygeoprocessing.raster_calculator(
            [(country_raster_path, 1), (country_nodata, 'raw'),
             (percentile_nodata0_task.get(), 'raw'),
             (PERCENTILE_RECLASS_LIST, 'raw'),
             (BIN_NODATA, 'raw')], bin_raster_op, bin_nodata0_raster_path,
            gdal.GDT_Float32, BIN_NODATA)
        stitch_queue.put((bin_raster_path, raster_id, 'nodata0'))

    task_graph.close()
    task_graph.join()


def process_country_percentile(
        file_lock, country_name, country_threshold_table_path,
        percentile_per_country_filename, raster_id, country_raster_path):
    """Calculate a single country/scenario percentile along with figure."""
    country_workspace = os.path.join(COUNTRY_WORKSPACES, country_name)
    percentile_values = pygeoprocessing.raster_band_percentile(
        (country_raster_path, 1), country_workspace, PERCENTILE_LIST)
    with file_lock:
        percentile_per_country_file = open(
            percentile_per_country_filename, 'a')
        percentile_per_country_file.write(
            '%s,' % country_name + ','.join(
                [str(x) for x in percentile_values]) + '\n')
        percentile_per_country_file.flush()
        percentile_per_country_file.close()

    if len(percentile_values) != len(PERCENTILE_LIST):
        return
    LOGGER.debug(
        "len percentile_values: %d len PERCENTILE_LIST: %d",
        len(percentile_values), len(PERCENTILE_LIST))

    cdf_array = [0.0] * len(percentile_values)

    nodata = pygeoprocessing.get_raster_info(
        country_raster_path)['nodata'][0]
    pixel_count = 0
    for _, data_block in pygeoprocessing.iterblocks(
            (country_raster_path, 1)):
        nodata_mask = ~numpy.isclose(data_block, nodata)
        pixel_count += numpy.count_nonzero(nodata_mask)
        for index, percentile_value in enumerate(percentile_values):
            cdf_array[index] += numpy.sum(data_block[
                nodata_mask & (data_block >= percentile_value)])

    # threshold is at 90% says Becky
    threshold_limit = 0.9 * cdf_array[2]

    LOGGER.debug(cdf_array)
    fig, ax = matplotlib.pyplot.subplots()
    ax.plot(list(reversed(PERCENTILE_LIST)), cdf_array)
    f = scipy.interpolate.interp1d(
        cdf_array, list(reversed(PERCENTILE_LIST)))
    try:
        cdf_threshold = f(threshold_limit)
    except ValueError:
        LOGGER.exception(
            "error when passing threshold_limit: %s\ncdf_array: %s" % (
                threshold_limit, cdf_array))
        cdf_threshold = cdf_array[2]

    ax.plot(
        [0, 100], [threshold_limit, threshold_limit], 'k:', linewidth=2)
    ax.plot([cdf_threshold, cdf_threshold],
            [cdf_array[0], cdf_array[-1]], 'k:', linewidth=2)

    ax.grid(True, linestyle='-.')
    ax.set_title(
        '%s CDF. 90%% max at %.2f and %.2f%%\nn=%d' % (
            country_name, threshold_limit, cdf_threshold, pixel_count))
    ax.set_ylabel('Sum of %s up to 100-percentile' % raster_id)
    ax.set_ylabel('100-percentile')
    ax.tick_params(labelcolor='r', labelsize='medium', width=3)
    matplotlib.pyplot.autoscale(enable=True, tight=True)
    matplotlib.pyplot.savefig(
        os.path.join(COUNTRY_WORKSPACES, '%s_%s_cdf.png' % (
            country_name, raster_id)))
    matplotlib.pyplot.close(fig)

    with file_lock:
        country_threshold_table_file = open(
            country_threshold_table_path, 'a')
        country_threshold_table_file.write(
            '%s, %f, %d\n' % (country_name, cdf_threshold, pixel_count))
        country_threshold_table_file.flush()
        country_threshold_table_file.close()


def extract_feature_checked(
        vector_path, field_name, field_value, base_raster_path,
        target_vector_path, target_raster_path):
    """Extract single feature into separate vector and check for no error.

    Parameters:
        vector_path (str): base vector in WGS84 coordinates.
        field_name (str): field to search for
        field_value (str): field value to isolate
        base_raster_path (str): path to raster to clip out.
        target_vector_path (str): path to new GPKG vector that will
            contain only that feature.
        target_raster_path (str): path to clipped out raster

    Returns:
        True if no error, False otherwise.

    """
    try:
        LOGGER.debug('opening vector: %s', vector_path)
        base_vector = gdal.OpenEx(vector_path, gdal.OF_VECTOR)
        LOGGER.debug('getting layer')
        base_layer = base_vector.GetLayer()
        feature = None
        LOGGER.debug('iterating over features')
        for base_feature in base_layer:
            if base_feature.GetField(field_name) == field_value:
                feature = base_feature
                break
        LOGGER.debug('extracting feature %s', feature.GetField(field_name))

        geom = feature.GetGeometryRef()
        base_srs = base_layer.GetSpatialRef()

        base_raster_info = pygeoprocessing.get_raster_info(base_raster_path)

        target_srs = osr.SpatialReference()
        target_srs.ImportFromWkt(base_raster_info['projection'])

        base_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        target_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

        base_to_target_transform = osr.CoordinateTransformation(
            base_srs, target_srs)

        base_layer = None
        base_vector = None

        # create a new shapefile
        if os.path.exists(target_vector_path):
            os.remove(target_vector_path)
        driver = ogr.GetDriverByName('GPKG')
        target_vector = driver.CreateDataSource(
            target_vector_path)
        target_layer = target_vector.CreateLayer(
            os.path.splitext(os.path.basename(target_vector_path))[0],
            target_srs, ogr.wkbMultiPolygon)
        layer_defn = target_layer.GetLayerDefn()
        feature_geometry = geom.Clone()
        base_feature = ogr.Feature(layer_defn)
        feature_geometry.Transform(base_to_target_transform)
        base_feature.SetGeometry(feature_geometry)
        target_layer.CreateFeature(base_feature)
        target_layer.SyncToDisk()
        geom = None
        feature_geometry = None
        base_feature = None
        target_layer = None
        target_vector = None

        pygeoprocessing.align_and_resize_raster_stack(
            [base_raster_path], [target_raster_path], ['near'],
            base_raster_info['pixel_size'], 'intersection',
            base_vector_path_list=[target_vector_path],
            vector_mask_options={
                'mask_vector_path': target_vector_path,
            })
        return True
    except Exception:
        LOGGER.exception('exception on extract vector')
        return False


def bin_raster_op(
        base_array, base_nodata, percentile_value_list,
        percentile_reclass_list, target_nodata):
    result = numpy.empty(base_array.shape, dtype=numpy.float32)
    result[:] = target_nodata
    # nodata is already "set"
    zero_mask = base_array == 0
    result[zero_mask] = 0
    set_so_far_mask = numpy.isclose(base_array, base_nodata) | zero_mask
    for value, reclass_value in zip(
            percentile_value_list[:-1],
            percentile_reclass_list[1:]):
        mask = (base_array < value) & ~set_so_far_mask
        result[mask] = reclass_value
        set_so_far_mask |= mask
    return result


# def raster_worker(work_queue, churn_dir):
#     """Process `work_queue` for gs:// paths or 'STOP'.

#     Parameters:
#         work_queue (queue): contains (raster_id, raster_uri) pairs or 'STOP'
#             sentinel. `raster_uri` can be in the format of `gs://`, `http`, or
#             a local file path. It must not be a pattern. If a cloud path it
#             will attempt to download that file first.

#     Returns:
#         None when reciving a 'STOP' in the work_queue.

#     """
#     LOGGER.debug(
#         'starting raster worker %s', threading.current_thread())

#     world_borders_vector_path = os.path.join(
#         WORKSPACE_DIR, os.path.basename(WORLD_BORDERS_URL))
#     world_borders_vector = gdal.OpenEx(world_borders_vector_path, gdal.OF_VECTOR)
#     world_borders_layer = world_borders_vector.GetLayer()

#     while True:
#         payload = work_queue.get()
#         if payload == 'STOP':
#             LOGGER.info('stopping %s', threading.current_thread())
#             work_queue.put('STOP')
#             return
#         raster_id, raster_uri = payload
#         if not os.path.exists(raster_uri):
#             raster_path = os.path.join(churn_dir, os.path.basename(raster_uri))
#             # note this will only re-download a file if it doesn't exist on disk
#             # this does not mean the file has not changed!
#             if not os.path.exists(raster_path):
#                 if raster_uri.startswith('gs://'):
#                     copy_from_gs(raster_uri, raster_path)
#                 elif raster_uri.startswith('http'):
#                     ecoshard.download_url(raster_uri, raster_path)
#                 else:
#                     raise ValueError(
#                         '%s is not a valid URI or filepath found on disk' %
#                         raster_uri)

#         # make a zeros removed raster

#         raster_info = pygeoprocessing.get_raster_info(raster_path)
#         country_threshold_table_path = os.path.join(
#             WORKSPACE_DIR, 'country_threshold_%s.csv' % raster_id)
#         country_threshold_table_file = open(country_threshold_table_path, 'w')
#         country_threshold_table_file.write(
#             'country,percentile at 90% max,pixel count\n')
#         country_threshold_table_file.close()
#         percentile_per_country_filename = '%s_percentile.csv' % raster_id
#         percentile_per_country_file = open(
#             percentile_per_country_filename, 'w')
#         percentile_per_country_file.write('country name,' + ','.join(
#             [str(x) for x in PERCENTILE_LIST]) + '\n')
#         percentile_per_country_file.close()

#         for world_border_feature in world_borders_layer:
#             country_name = world_border_feature.GetField('nev_name')
#             LOGGER.debug(country_name)
#             country_workspace = os.path.join(COUNTRY_WORKSPACES, country_name)
#             try:
#                 os.makedirs(country_workspace)
#             except OSError:
#                 pass

#             country_vector = os.path.join(
#                 country_workspace, '%s.gpkg' % country_name)
#             country_vector_complete_token = os.path.join(
#                 country_workspace, '%s.COMPLETE' % country_name)
#             extract_task = task_graph.add_task(
#                 func=extract_feature,
#                 args=(
#                     world_borders_vector_path, world_border_feature.GetFID(),
#                     wgs84_srs.ExportToWkt(), country_vector,
#                     country_vector_complete_token),
#                 target_path_list=[country_vector_complete_token],
#                 task_name='exctract %s' % country_name)

#             country_raster_path = os.path.join(country_workspace, '%s_%s' % (
#                 country_name, os.path.basename(raster_path)))

#             country_vector_info = pygeoprocessing.get_vector_info(
#                 country_vector)
#             task_graph.add_task(
#                 func=pygeoprocessing.warp_raster,
#                 args=(
#                     raster_path, raster_info['pixel_size'],
#                     country_raster_path, 'near'),
#                 kwargs={
#                     'target_bb': country_vector_info['bounding_box'],
#                     'vector_mask_options': {
#                         'mask_vector_path': country_vector},
#                     'working_dir': country_workspace},
#                 target_path_list=[country_raster_path],
#                 dependent_task_list=[extract_task],
#                 task_name='warp %s' % country_name)
#             country_raster_path_list.append(
#                 (country_name, country_threshold_table_path,
#                  percentile_per_country_filename, raster_id,
#                  country_raster_path))


def copy_from_gs(gs_uri, target_path):
    """Copy a GS objec to `target_path."""
    dirpath = os.path.dirname(target_path)
    try:
        os.makedirs(dirpath)
    except Exception:
        pass
    subprocess.run(
        'gsutil cp %s %s' % (gs_uri, target_path), shell=True, check=True)


def get_value_list(vector_path, fieldname):
    """Returns a list of values of each features fieldname."""
    vector = gdal.OpenEx(vector_path, gdal.OF_VECTOR)
    layer = vector.GetLayer()
    value_list = [
        feature.GetField(fieldname)
        for feature in layer]
    layer = None
    vector = None
    return value_list


def gs_copy(gs_path, target_path):
    """Copy gs_path to target_path."""
    LOGGER.debug('about to gs copy %s to %s', gs_path, target_path)
    subprocess.run(
        'gsutil cp %s %s' % (gs_path, target_path), shell=True, check=True)


def main():
    """Entry point.

    Returns:
        None.

    """
    for dir_path in [
            WORKSPACE_DIR, ECOSHARD_DIR, CHURN_DIR, COUNTRY_WORKSPACES]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    task_graph = taskgraph.TaskGraph(CHURN_DIR, 4)
    LOGGER.info('starting `main`')
    world_borders_vector_path = os.path.join(
        ECOSHARD_DIR, os.path.basename(WORLD_BORDERS_URL))
    download_world_borders_task = task_graph.add_task(
        func=ecoshard.download_url,
        args=(WORLD_BORDERS_URL, world_borders_vector_path),
        hash_target_files=False,
        target_path_list=[world_borders_vector_path],
        task_name='download world borders')
    download_world_borders_task.join()

    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)

    result = subprocess.run(
        'gsutil ls -p ecoshard %s' % BUCKET_PATTERN, capture_output=True,
        shell=True, check=True)
    gs_path_list = [
        x.decode('utf-8') for x in result.stdout.splitlines()
        if 'reef' not in x.decode('utf-8').lower()]
    raster_id_list = [
        os.path.basename(os.path.splitext(gs_path)[0])
        for gs_path in gs_path_list]

    LOGGER.debug(raster_id_list)

    country_id_task = task_graph.add_task(
        func=get_value_list,
        args=(world_borders_vector_path, COUNTRY_ID_FIELDNAME),
        ignore_path_list=[world_borders_vector_path],
        dependent_task_list=[download_world_borders_task],
        task_name='fetch country ids')

    country_id_task.join()
    country_id_list = country_id_task.get()
    LOGGER.debug('country id list: %s', country_id_list)

    create_status_database_task = task_graph.add_task(
        func=create_status_database,
        args=(WORK_DATABASE_PATH, raster_id_list,
              country_id_list),
        target_path_list=[WORK_DATABASE_PATH],
        hash_target_files=False,
        task_name='initalize work database')
    LOGGER.debug('create work database')
    create_status_database_task.join()

    raster_id_to_path_map = {}
    LOGGER.debug('copy gs files')
    for gs_path in gs_path_list:
        LOGGER.debug('copy %s', gs_path)
        raster_id = os.path.basename(os.path.splitext(gs_path)[0])
        target_raster_path = os.path.join(CHURN_DIR, os.path.basename(gs_path))
        raster_id_to_path_map[raster_id] = target_raster_path
        _ = task_graph.add_task(
            func=gs_copy,
            args=(gs_path, target_raster_path),
            target_path_list=[target_raster_path],
            task_name='gs copy %s' % gs_path)
    task_graph.join()
    result = _execute_sqlite(
        'SELECT raster_id, country_id FROM job_status''',
        WORK_DATABASE_PATH, execute='execute', argument_list=[], fetch='all')

    task_graph.join()
    raster_id_to_global_stitch_path_map = {}
    for raster_id, raster_path in raster_id_to_path_map.items():
        for nodata_id in ['', 'nodata0']:
            global_stitch_raster_path = os.path.join(
                WORKSPACE_DIR, '%s%s_by_country.tif' % (raster_id, nodata_id))
            raster_id_to_global_stitch_path_map[(raster_id, nodata_id)] = (
                global_stitch_raster_path)
            raster_info = pygeoprocessing.get_raster_info(
                raster_path)
            task_graph.add_task(
                func=pygeoprocessing.new_raster_from_base,
                args=(
                    raster_path, global_stitch_raster_path,
                    raster_info['datatype'], [raster_info['nodata'][0]]),
                kwargs={'fill_value_list': [raster_info['nodata'][0]]},
                ignore_path_list=[global_stitch_raster_path],
                task_name='make empty stitch raster for %s%s' % (
                    raster_id, nodata_id))
    task_graph.join()

    work_queue = multiprocessing.Queue()
    # TODO: iterate by country size from largest to smallest, including no country first
    for raster_id, country_id in result:
        work_queue.put((raster_id, country_id))
    work_queue.put(None)

    stitch_queue = multiprocessing.Queue()
    worker_list = []
    for worker_id in range(max(1, multiprocessing.cpu_count())):
        country_worker_process = multiprocessing.Process(
            target=process_country_worker,
            args=(
                work_queue, world_borders_vector_path, raster_id_to_path_map,
                stitch_queue),
            name='%d' % worker_id)
        country_worker_process.start()
        worker_list.append(country_worker_process)

    stitch_worker_process = multiprocessing.Process(
        target=stitch_worker,
        args=(stitch_queue, raster_id_to_global_stitch_path_map),
        name='stitch worker')
    stitch_worker_process.start()

    work_queue.put('STOP')
    for process in worker_list:
        process.join()

    stitch_queue.put('STOP')
    stitch_worker_process.join()

    # TODO: percentile csv tables for
    #   - regular, nodata set to 0
    for raster_id, raster_path in raster_id_to_path_map.items():
        result = _execute_sqlite(
            '''
            SELECT percentile_list, percentile0_list, country_id
            FROM job_status
            WHERE raster_id=?;
            ''',
            WORK_DATABASE_PATH, execute='execute', argument_list=['raster_id'],
            fetch='all')

        percentile_map = {
            (country_id, (percentile_list, percentile0_list))
            for (country_id, percentile_list, percentile0_list) in result
        }
        world_percentile_list = percentile_map[None][0]
        world_nodata0_percentile_list = percentile_map[None][1]
        del percentile_map[None]

        csv_percentile_path = os.path.join(
            WORKSPACE_DIR, '%s_percentile.csv' % raster_id)
        csv_nodata0_percentile_path = os.path.join(
            WORKSPACE_DIR, '%s_nodata0_percentile.csv' % raster_id)

        with open(csv_percentile_path, 'w') as csv_percentile_file:
            csv_percentile_file.write('%s percentiles\n' % raster_id)
            csv_percentile_file.write(
                'country,' +
                ','.join([str(x) for x in PERCENTILE_LIST]))
            # first do the whole world
            csv_percentile_file.write(
                'world,' +
                ','.join([str(x) for x in world_percentile_list]))
            for country_id in sorted(percentile_map):
                csv_percentile_file.write(
                    '%s,' % country_id +
                    ','.join([str(x) for x in percentile_map[country_id][0]]))

        with open(csv_nodata0_percentile_path, 'w') as \
                csv_nodata0_percentile_file:
            csv_nodata0_percentile_file.write('%s percentiles\n' % raster_id)
            csv_nodata0_percentile_file.write(
                'country,' +
                ','.join([str(x) for x in PERCENTILE_LIST]))
            # first do the whole world
            csv_nodata0_percentile_file.write(
                'world,' +
                ','.join([str(x) for x in world_nodata0_percentile_list]))
            for country_id in sorted(percentile_map):
                csv_nodata0_percentile_file.write(
                    '%s,' % country_id +
                    ','.join([str(x) for x in percentile_map[country_id][1]]))

    task_graph.close()
    task_graph.join()

    # TODO: global binning (already do country binning)


def stitch_worker(stitch_queue, raster_id_to_global_stitch_path_map):
    """Stitch incoming country rasters into global raster.

    Parameters:
        stitch_queue (queue): payloads come in as an alert that a sub raster
            is ready for stitching into the global raster. Payloads are of the
            form `base_raster_path, raster_id, nodata_flag` where the
            `raster_id` is indexed into `raster_id_to_global_stitch_path_map`
            and the `nodata_flag` is indexed into
            `raster_id_to_global_stitch_path_map[(raster_id, nodata_flag)]`.
        raster_id_to_global_stitch_path_map (dict): dictionary indexed by
            raster id and nodata flag tuple to the global raster.

    """
    try:
        while True:
            payload = stitch_queue.get()
            if payload == 'STOP':
                stitch_queue.put('STOP')
                break
            local_tile_raster_path, raster_id, nodata_flag = payload
            global_stitch_raster_path = \
                raster_id_to_global_stitch_path_map[(raster_id, nodata_flag)]

            # get ul of tile and figure out where it goes in global
            local_tile_info = pygeoprocessing.get_raster_info(
                local_tile_raster_path)
            global_stitch_info = pygeoprocessing.get_raster_info(
                global_stitch_raster_path)
            global_inv_gt = gdal.InvGeoTransform(
                global_stitch_info['geotransform'])
            local_gt = local_tile_info['geotransform']
            global_i, global_j = gdal.ApplyGeoTransform(
                global_inv_gt, local_gt[0], local_gt[3])

            local_tile_raster = gdal.OpenEx(
                local_tile_raster_path, gdal.OF_RASTER)
            local_array = local_tile_raster.ReadAsArray()
            local_tile_raster = None
            global_raster = gdal.OpenEx(
                global_stitch_raster_path, gdal.OF_RASTER | gdal.GA_Update)
            global_band = global_raster.GetRasterBand(1)
            global_array = global_band.ReadAsArray(
                xoff=global_i, yoff=global_j,
                win_xsize=local_array.shape[1], win_ysize=local_array.shape[0])
            valid_mask = ~numpy.isclose(
                local_array, local_tile_info['nodata'][0])
            global_array[valid_mask] = local_array[valid_mask]
            global_band.WriteArray(global_array, xoff=global_i, yoff=global_j)
            global_band.FlushCache()
            global_band = None
            global_raster = None
    except Exception:
        LOGGER.exception('exception on stitch worker')


if __name__ == '__main__':
    main()
