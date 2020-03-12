"""
Count non-zero non-nodata pixels in raster, get percentile values, and sum
above each percentile to build the CDF.
"""
import ecoshard
import itertools
import logging
import logging.handlers
import multiprocessing
import os
import pickle
import sqlite3
import subprocess
import time

import numpy
import pygeoprocessing
from osgeo import gdal
from osgeo import osr
from osgeo import ogr
import retrying
from taskgraph.Task import _execute_sqlite
import taskgraph

gdal.SetCacheMax(2**30)

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
        filename='log.txt')
LOGGER = logging.getLogger(__name__)
logging.getLogger('matplotlib').setLevel(logging.ERROR)
logging.getLogger('taskgraph').setLevel(logging.INFO)

BUCKET_PATTERN = 'gs://shared-with-users/realized_services/*.tif'
WORLD_BORDERS_URL = (
    'https://storage.googleapis.com/critical-natural-capital-ecoshards/'
    'countries_iso3_md5_6fb2431e911401992e6e56ddf0a9bcda.gpkg')

EEZ_URL = (
    'https://storage.googleapis.com/critical-natural-capital-ecoshards/'
    'eez_v11_md5_72307ea605d6712bf79618f33e67676e.gpkg')

COUNTRY_ID_FIELDNAME = 'iso3'
GLOBAL_COUNTRY_NAME = '_GLOBAL'

PERCENTILE_LIST = list(range(0, 101, 1))
PERCENTILE_RECLASS_LIST = [
    i/(len(PERCENTILE_LIST)-1) * 10
    for i in range(len(PERCENTILE_LIST))]
BIN_NODATA = -1
WORK_DATABASE_PATH = os.path.join(CHURN_DIR, 'work_status.db')

SKIP_THESE_COUNTRIES = ['ATA']


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
            cdf BLOB,
            cdfnodata0 BLOB);
        """)
    if os.path.exists(database_path):
        os.remove(database_path)
    connection = sqlite3.connect(database_path)
    connection.executescript(create_database_sql)

    # insert global
    connection.executemany(
        'INSERT INTO job_status(raster_id, country_id, is_country) '
        'VALUES (?, ?, 0)',
        [(x, GLOBAL_COUNTRY_NAME) for x in raster_id_list])
    # insert countries
    connection.executemany(
        'INSERT INTO job_status(raster_id, country_id, is_country) '
        'VALUES (?, ?, 1)',
        itertools.product(raster_id_list, country_id_list))
    connection.commit()
    connection.close()


@retrying.retry()
def process_country_worker(
        feature_lock, work_queue, world_border_vector_path,
        raster_id_to_path_map, stitch_queue):
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
    try:
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
                    ignore_path_list=[
                        world_border_vector_path, country_vector_path],
                    target_path_list=[country_raster_path],
                    task_name='extract vector %s' % country_id)

                if not extract_feature_checked_task.get():
                    with open(os.path.join(worker_dir, 'error.txt'), 'w') as \
                            error_file:
                        error_file.write(
                            'extraction not work %s:%s' % (
                                country_id, base_raster_info))
                    continue
            else:
                country_raster_path = raster_id_to_path_map[raster_id]
                country_id = GLOBAL_COUNTRY_NAME

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

            cdf_task = task_graph.add_task(
                func=calculate_cdf,
                args=(country_raster_path, percentile_task.get()),
                task_name='calculate cdf for %s' % country_raster_path)

            cdf_nodata0_task = task_graph.add_task(
                func=calculate_cdf,
                args=(country_nodata0_raster_path,
                      percentile_nodata0_task.get()),
                task_name='calculate cdf for %s' % country_nodata0_raster_path)

            _execute_sqlite(
                '''
                    UPDATE job_status
                    SET
                      percentile_list=?, percentile0_list=?,
                      cdf=?, cdfnodata0=?
                    WHERE raster_id=? and country_id=?
                ''',
                WORK_DATABASE_PATH, execute='execute', mode='modify',
                argument_list=[
                    pickle.dumps(percentile_task.get()),
                    pickle.dumps(percentile_nodata0_task.get()),
                    pickle.dumps(cdf_task.get()),
                    pickle.dumps(cdf_nodata0_task.get()),
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
                 (percentile_task.get(), 'raw'),
                 (PERCENTILE_RECLASS_LIST, 'raw'), (BIN_NODATA, 'raw')],
                bin_raster_op, bin_raster_path,
                gdal.GDT_Float32, BIN_NODATA)
            LOGGER.debug(
                'stitch this: %s', str((bin_raster_path, raster_id, '')))
            stitch_queue.put((bin_raster_path, raster_id, ''))

            if country_id:
                bin_nodata0_raster_path = os.path.join(
                    worker_dir, 'bin_nodata0_raster.tif')
            else:
                # it's global
                bin_nodata0_raster_path = os.path.join(
                    WORKSPACE_DIR, '%s_bin_nodata0_raster.tif' % raster_id)

            # the first argument is supposed to be `country_raster_path` since
            # we want to leave the 0s in there even though the percentiles are
            # different
            pygeoprocessing.raster_calculator(
                [(country_raster_path, 1), (country_nodata, 'raw'),
                 (percentile_nodata0_task.get(), 'raw'),
                 (PERCENTILE_RECLASS_LIST, 'raw'),
                 (BIN_NODATA, 'raw')], bin_raster_op, bin_nodata0_raster_path,
                gdal.GDT_Float32, BIN_NODATA)
            LOGGER.debug('stitch this: %s', str(
                (bin_raster_path, raster_id, 'nodata0')))
            stitch_queue.put((bin_raster_path, raster_id, 'nodata0'))

    except Exception:
        LOGGER.exception(
            'exception on process_country_worker for %s %s' % (
                raster_id, country_id))
        raise
    finally:
        task_graph.close()
        task_graph.join()


def extract_feature_checked(
        vector_path, field_name, field_value, base_raster_path,
        target_vector_path, target_raster_path):
    """Extract single feature into separate vector and check for no error.

    Do not do a transform since it's all wgs84.

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
    attempt_number = 0
    while True:
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

            base_raster_info = pygeoprocessing.get_raster_info(
                base_raster_path)

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
                base_srs, ogr.wkbMultiPolygon)
            layer_defn = target_layer.GetLayerDefn()
            feature_geometry = geom.Clone()
            base_feature = ogr.Feature(layer_defn)
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
            attempt_number += 1
            if attempt_number == 20:
                return False
            time.sleep(min(1, 0.1*2**attempt_number))


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
            percentile_value_list,
            percentile_reclass_list):
        mask = (base_array <= value) & ~set_so_far_mask
        result[mask] = reclass_value
        set_so_far_mask |= mask
    return result


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

    task_graph = taskgraph.TaskGraph(
            CHURN_DIR, multiprocessing.cpu_count(), 5.0)
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
            global_stitch_raster_id = (
                '%s%s_by_country' % (raster_id, nodata_id))
            global_stitch_raster_path = os.path.join(
                WORKSPACE_DIR, '%s.tif' % global_stitch_raster_id)
            raster_id_to_global_stitch_path_map[(raster_id, nodata_id)] = (
                global_stitch_raster_path)
            raster_info = pygeoprocessing.get_raster_info(
                raster_path)
            task_graph.add_task(
                func=new_raster_from_base,
                args=(
                    raster_path, global_stitch_raster_id, WORKSPACE_DIR,
                    raster_info['datatype'], raster_info['nodata'][0]),
                hash_target_files=False,
                target_path_list=[global_stitch_raster_path],
                task_name='make empty stitch raster for %s%s' % (
                    raster_id, nodata_id))

    task_graph.close()
    task_graph.join()

    work_queue = multiprocessing.Queue()
    for raster_id, country_id in result:
        if country_id in SKIP_THESE_COUNTRIES:
            continue
        # TODO: this is temporarily set to global only just so we can get
        # values
        if country_id != GLOBAL_COUNTRY_NAME:
            continue
        LOGGER.debug('putting %s %s to work', raster_id, country_id)
        work_queue.put((raster_id, country_id))

    work_queue.put('STOP')

    stitch_queue = multiprocessing.Queue()
    feature_lock = multiprocessing.Lock()
    worker_list = []
    for worker_id in range(max(1, multiprocessing.cpu_count())):
        country_worker_process = multiprocessing.Process(
            target=process_country_worker,
            args=(
                feature_lock, work_queue, world_borders_vector_path,
                raster_id_to_path_map, stitch_queue),
            name='%d' % worker_id)
        country_worker_process.start()
        worker_list.append(country_worker_process)

    raster_id_lock_map = {
        raster_id: multiprocessing.Lock()
        for raster_id in raster_id_to_global_stitch_path_map
    }

    stitch_worker_list = []
    for worker_id in range(max(1, multiprocessing.cpu_count())):
        stitch_worker_process = multiprocessing.Process(
            target=stitch_worker,
            args=(stitch_queue, raster_id_to_global_stitch_path_map,
                  raster_id_lock_map),
            name='stitch worker %s' % worker_id)
        stitch_worker_process.start()
        stitch_worker_list.append(stitch_worker_process)

    LOGGER.debug('wait for workers to stop')
    for process in worker_list:
        process.join()

    LOGGER.debug('workers stopped')
    stitch_queue.put('STOP')

    LOGGER.debug('building histogram/cdf')
    for raster_id, raster_path in raster_id_to_path_map.items():
        LOGGER.debug('building csv for %s %s', raster_id, raster_path)
        result = _execute_sqlite(
            '''
            SELECT
              country_id, percentile_list, percentile0_list, cdf, cdfnodata0
            FROM job_status
            WHERE raster_id=?;
            ''',
            WORK_DATABASE_PATH, execute='execute', argument_list=[raster_id],
            fetch='all')

        percentile_map = {
            country_id: (
                pickle.loads(percentile_list),
                pickle.loads(percentile0_list),
                pickle.loads(cdf),
                pickle.loads(cdfnodata0))
            for (country_id, percentile_list, percentile0_list,
                 cdf, cdfnodata0) in result
            if None not in (
                country_id, percentile_list, percentile0_list, cdf, cdfnodata0)
        }

        csv_percentile_path = os.path.join(
            WORKSPACE_DIR, '%s_percentile.csv' % raster_id)
        csv_nodata0_percentile_path = os.path.join(
            WORKSPACE_DIR, '%s_nodata0_percentile.csv' % raster_id)

        csv_cdf_path = os.path.join(
            WORKSPACE_DIR, '%s_cdf.csv' % raster_id)
        csv_nodata0_cdf_path = os.path.join(
            WORKSPACE_DIR, '%s_nodata0_cdf.csv' % raster_id)

        with open(csv_cdf_path, 'w') as csv_cdf_file:
            csv_cdf_file.write('%s cdfs' % raster_id)
            csv_cdf_file.write(
                '\ncountry,' +
                ','.join([str(x) for x in PERCENTILE_LIST]))
            # first do the whole world
            for country_id in sorted(percentile_map):
                csv_cdf_file.write(
                    '\n%s,' % country_id +
                    ','.join(reversed([
                        str(x) for x in percentile_map[country_id][2]])))

        with open(csv_nodata0_cdf_path, 'w') as csv_cdf_nodata0_file:
            csv_cdf_nodata0_file.write('%s cdfs' % raster_id)
            csv_cdf_nodata0_file.write(
                '\ncountry,' +
                ','.join([str(x) for x in PERCENTILE_LIST]))
            # first do the whole world
            for country_id in sorted(percentile_map):
                csv_cdf_nodata0_file.write(
                    '\n%s,' % country_id +
                    ','.join(reversed([
                        str(x) for x in percentile_map[country_id][3]])))

        with open(csv_percentile_path, 'w') as csv_percentile_file:
            csv_percentile_file.write('%s percentiles' % raster_id)
            csv_percentile_file.write(
                '\ncountry,' +
                ','.join([str(x) for x in PERCENTILE_LIST]))
            # first do the whole world
            for country_id in sorted(percentile_map):
                csv_percentile_file.write(
                    '\n%s,' % country_id +
                    ','.join([str(x) for x in percentile_map[country_id][0]]))

        with open(csv_nodata0_percentile_path, 'w') as \
                csv_nodata0_percentile_file:
            csv_nodata0_percentile_file.write('%s percentiles' % raster_id)
            csv_nodata0_percentile_file.write(
                '\ncountry,' +
                ','.join([str(x) for x in PERCENTILE_LIST]))
            # first do the whole world
            for country_id in sorted(percentile_map):
                csv_nodata0_percentile_file.write(
                    '\n%s,' % country_id +
                    ','.join([str(x) for x in percentile_map[country_id][1]]))

    LOGGER.debug('wait for stitch to stop')
    for stitch_worker_process in stitch_worker_list:
        stitch_worker_process.join()
    LOGGER.debug('stitch stopped')
    LOGGER.info('ALL DONE!')


def calculate_cdf(raster_path, percentile_list):
    """Calculate the CDF given its percentile list."""
    cdf_array = [0.0] * len(percentile_list)
    nodata = pygeoprocessing.get_raster_info(
        raster_path)['nodata'][0]
    pixel_count = 0
    for _, data_block in pygeoprocessing.iterblocks(
            (raster_path, 1)):
        nodata_mask = ~numpy.isclose(data_block, nodata)
        pixel_count += numpy.count_nonzero(nodata_mask)
        for index, percentile_value in enumerate(percentile_list):
            cdf_array[index] += numpy.sum(data_block[
                nodata_mask & (data_block >= percentile_value)])
    return cdf_array


def stitch_worker(
        stitch_queue, raster_id_to_global_stitch_path_map,
        raster_id_lock_map):
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
        raster_id_lock_map (dict): mapping raster ids to multiprocessing Locks
            so we don't edit more than one raster at a time.

    """
    try:
        while True:
            payload = stitch_queue.get()
            if payload == 'STOP':
                LOGGER.info('stopping stitch_worker')
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
            valid_mask = ~numpy.isclose(
                local_array, local_tile_info['nodata'][0])
            if valid_mask.size == 0:
                continue
            with raster_id_lock_map[raster_id]:
                global_raster = gdal.OpenEx(
                    global_stitch_raster_path, gdal.OF_RASTER | gdal.GA_Update)
                LOGGER.debug(
                    'stitching this %s into this %s',
                    global_stitch_raster_path, payload)
                global_band = global_raster.GetRasterBand(1)
                global_array = global_band.ReadAsArray(
                    xoff=global_i, yoff=global_j,
                    win_xsize=local_array.shape[1],
                    win_ysize=local_array.shape[0])
                global_array[valid_mask] = local_array[valid_mask]
                win_ysize_write, win_xsize_write = global_array.shape
                if win_ysize_write == 0 or win_xsize_write == 0:
                    LOGGER.debug(
                        'got zeros on sizes: %d %d %s',
                        win_ysize_write, win_xsize_write, payload)
                    continue
                if global_i + win_xsize_write >= global_band.XSize:
                    win_xsize_write = int(global_band.XSize - global_i)
                if global_j + win_ysize_write >= global_band.YSize:
                    win_ysize_write = int(global_band.YSize - global_j)

                global_band.WriteArray(
                    global_array[0:win_ysize_write, 0:win_xsize_write],
                    xoff=global_i, yoff=global_j)
                global_band.FlushCache()
                global_band = None
                global_raster = None
    except Exception:
        LOGGER.exception('exception on stitch worker')


def new_raster_from_base(
        base_raster, target_base_id, target_dir, target_datatype,
        target_nodata):
    """Create a new raster from base given the base id.

    This function is to make the function signature look different for each
    run.

    Parameters:
        base_raster, target_base_id, target_dir, target_datatype,
        target_nodata are the same as pygeoprocessing.new_raster_from_base.

    Returns:
        None.

    """
    target_raster_path = os.path.join(target_dir, '%s.tif' % target_base_id)
    pygeoprocessing.new_raster_from_base(
        base_raster, target_raster_path,
        target_datatype, [target_nodata])


if __name__ == '__main__':
    main()
