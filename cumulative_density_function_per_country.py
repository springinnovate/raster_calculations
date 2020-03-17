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
import pathlib
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
import taskgraph

gdal.SetCacheMax(2**30)
gdal.UseExceptions()

WORKSPACE_DIR = 'cdf_by_country'
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshard')
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')
COUNTRY_WORKSPACES = os.path.join(WORKSPACE_DIR, 'country_workspaces')
NCPUS = multiprocessing.cpu_count()

logging.basicConfig(
        level=logging.DEBUG,
        format=(
            '%(asctime)s (%(relativeCreated)d) %(processName)s %(levelname)s '
            '%(name)s [%(funcName)s:%(lineno)d] %(message)s'),
        filename='log.txt')
LOGGER = logging.getLogger(__name__)
logging.getLogger('matplotlib').setLevel(logging.ERROR)
logging.getLogger('taskgraph').setLevel(logging.INFO)

WORK_MAP = {
    'world_borders': {
        'vector_url':  (
            'https://storage.googleapis.com/'
            'critical-natural-capital-ecoshards/'
            'countries_iso3_md5_6fb2431e911401992e6e56ddf0a9bcda.gpkg'),
        'fieldname_id': 'iso3',
        'raster_gs_pattern':
            'gs://shared-with-users/realized_services/terrestrial/realized_fwfish_distrib_catch*.tif'
            #'gs://shared-with-users/realized_services/terrestrial/*.tif'
    },
    # Becky wants me to skip the EEZ zones.
    # 'eez': {
    #     'vector_url':  (
    #         'https://storage.googleapis.com/'
    #         'critical-natural-capital-ecoshards/'
    #         'eez_v11_md5_72307ea605d6712bf79618f33e67676e.gpkg'),
    #     'fieldname_id': 'ISO_SOV1',
    #     'raster_gs_pattern':
    #         'gs://shared-with-users/realized_services/marine/*.tif'
    # }
}

GLOBAL_ID = '_GLOBAL'

PERCENTILE_LIST = list(range(0, 101, 1))
PERCENTILE_RECLASS_LIST = [
    i/(len(PERCENTILE_LIST)-1) * 10
    for i in range(len(PERCENTILE_LIST))]
BIN_NODATA = -1
WORK_DATABASE_PATH = os.path.join(CHURN_DIR, 'work_status.db')

SKIP_THESE_FEATURE_IDS = ['ATA']


def country_nodata0_op(base_array, nodata):
    """Convert base_array 0s to nodata."""
    result = base_array.copy()
    result[base_array == 0] = nodata
    return result


def create_status_database(database_path):
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
            aggregate_vector_id TEXT NOT NULL,
            fieldname_id TEXT NOT NULL,
            feature_id TEXT NOT NULL,
            percentile_list BLOB,
            percentile0_list BLOB,
            cdf BLOB,
            cdfnodata0 BLOB);

        CREATE UNIQUE INDEX unique_job_status ON
        job_status (raster_id, aggregate_vector_id, fieldname_id, feature_id);
        """)
    if os.path.exists(database_path):
        os.remove(database_path)
    connection = sqlite3.connect(database_path)
    connection.executescript(create_database_sql)
    connection.commit()
    connection.close()


@retrying.retry()
def feature_worker(
        work_queue, align_lock, aggregate_vector_id_to_path,
        raster_id_to_path_map, stitch_queue):
    """Process work queue.

    Parameters:
        work_queue (queue): expect (raster_id, aggregate_vector_id, feature_id)
            tuples. 'feature_id' can be GLOBAL_FEATURE_ID which means do the
            whole raster. If 'STOP', shut down.
        align_lock (multiprocessing.Lock): to ensure only one align per time
            so GDAL doesn't get confused on lookup for coordinate systems.
        aggregate_vector_id_to_path (dict): a dictionary mapping aggregate
            ids to vector paths for per-feature normalization.
        raster_id_to_path_map (dict): maps 'raster_id' to paths to rasters on
            disk.
        stitch_queue (queue): when a country tuple is complete push a
            (bin_path, raster_id, nodata0) tuple down it.

    """
    LOGGER.debug('starting feature_worker')
    task_graph = taskgraph.TaskGraph(CHURN_DIR, -1)
    try:
        while True:
            payload = work_queue.get()
            if payload == 'STOP':
                work_queue.put('STOP')
                LOGGER.debug('stopping')
                break
            raster_id, aggregate_vector_id, feature_id, fieldname_id = payload
            LOGGER.debug(
                'got %s:%s:%s', raster_id, aggregate_vector_id, feature_id)
            worker_dir = os.path.join(
                COUNTRY_WORKSPACES, '%s_%s_%s' % (
                    raster_id, aggregate_vector_id, feature_id))
            try:
                os.makedirs(worker_dir)
            except OSError:
                pass

            if feature_id != GLOBAL_ID:
                local_aggregate_vector_path = os.path.join(
                    worker_dir, '%s_%s.gpkg' % (
                        aggregate_vector_id, feature_id))
                LOGGER.debug(
                    'making aggregate vector %s', local_aggregate_vector_path)
                base_raster_info = pygeoprocessing.get_raster_info(
                    raster_id_to_path_map[
                        (raster_id, aggregate_vector_id, fieldname_id)])
                feature_raster_path = '%s.tif' % os.path.splitext(
                    local_aggregate_vector_path)[0]

                extract_feature_checked_task = task_graph.add_task(
                    func=extract_feature_checked,
                    args=(
                        align_lock,
                        aggregate_vector_id_to_path[aggregate_vector_id],
                        fieldname_id, feature_id,
                        raster_id_to_path_map[
                            (raster_id, aggregate_vector_id, fieldname_id)],
                        local_aggregate_vector_path, feature_raster_path),
                    ignore_path_list=[
                        aggregate_vector_id_to_path[aggregate_vector_id],
                        local_aggregate_vector_path],
                    target_path_list=[feature_raster_path],
                    task_name='extract vector %s' % feature_id)

                if not extract_feature_checked_task.get():
                    with open(os.path.join(worker_dir, 'error.txt'), 'w') as \
                            error_file:
                        error_file.write(
                            'extraction not work %s:%s' % (
                                feature_id, base_raster_info))
                    continue
            else:
                feature_raster_path = raster_id_to_path_map[
                    (raster_id, aggregate_vector_id, fieldname_id)]

            working_sort_directory = os.path.join(worker_dir, 'percentile_reg')
            percentile_task = task_graph.add_task(
                func=pygeoprocessing.raster_band_percentile,
                args=(
                    (feature_raster_path, 1), working_sort_directory,
                    PERCENTILE_LIST),
                task_name='percentile for %s' % working_sort_directory)
            LOGGER.debug('percentile_task: %s', percentile_task.get())

            country_nodata0_raster_path = '%s_nodata0.tif' % os.path.splitext(
                feature_raster_path)[0]
            feature_raster_info = pygeoprocessing.get_raster_info(
                feature_raster_path)
            country_nodata = feature_raster_info['nodata'][0]
            nodata0_raster_task = task_graph.add_task(
                func=pygeoprocessing.raster_calculator,
                args=(
                    [(feature_raster_path, 1), (country_nodata, 'raw')],
                    country_nodata0_op, country_nodata0_raster_path,
                    feature_raster_info['datatype'], country_nodata),
                target_path_list=[country_nodata0_raster_path],
                task_name='set zero to nodata for %s' % feature_raster_path)

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
                args=(feature_raster_path, percentile_task.get()),
                task_name='calculate cdf for %s' % feature_raster_path)

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
                    WHERE
                        raster_id=? AND aggregate_vector_id=? AND
                        feature_id=? AND fieldname_id=?
                ''',
                WORK_DATABASE_PATH, execute='execute', mode='modify',
                argument_list=[
                    pickle.dumps(percentile_task.get()),
                    pickle.dumps(percentile_nodata0_task.get()),
                    pickle.dumps(cdf_task.get()),
                    pickle.dumps(cdf_nodata0_task.get()),
                    raster_id, aggregate_vector_id, feature_id, fieldname_id])

            LOGGER.debug(
                'percentile_nodata0_task: %s', percentile_nodata0_task.get())
            if feature_id != GLOBAL_ID:
                bin_raster_path = os.path.join(worker_dir, 'bin_raster.tif')
            else:
                # it's global
                bin_raster_path = os.path.join(
                    WORKSPACE_DIR, '%s_%s_%s_bin_raster.tif' % (
                        raster_id, aggregate_vector_id, feature_id))
            pygeoprocessing.raster_calculator(
                [(feature_raster_path, 1), (country_nodata, 'raw'),
                 (percentile_task.get(), 'raw'),
                 (PERCENTILE_RECLASS_LIST, 'raw'), (BIN_NODATA, 'raw')],
                bin_raster_op, bin_raster_path,
                gdal.GDT_Float32, BIN_NODATA)
            LOGGER.debug('stitch this: %s', str(
                (bin_raster_path, aggregate_vector_id, raster_id, '')))
            stitch_queue.put(
                (bin_raster_path, (raster_id, aggregate_vector_id, '')))

            if feature_id != GLOBAL_ID:
                bin_nodata0_raster_path = os.path.join(
                    worker_dir, 'bin_nodata0_raster.tif')
            else:
                # it's global
                bin_nodata0_raster_path = os.path.join(
                    WORKSPACE_DIR, '%s_%s_%s_bin_nodata0_raster.tif' % (
                        raster_id, aggregate_vector_id, feature_id))

            # the first argument is supposed to be `feature_raster_path` since
            # we want to leave the 0s in there even though the percentiles are
            # different
            pygeoprocessing.raster_calculator(
                [(feature_raster_path, 1), (country_nodata, 'raw'),
                 (percentile_nodata0_task.get(), 'raw'),
                 (PERCENTILE_RECLASS_LIST, 'raw'),
                 (BIN_NODATA, 'raw')], bin_raster_op, bin_nodata0_raster_path,
                gdal.GDT_Float32, BIN_NODATA)
            LOGGER.debug('stitch this: %s', str(
                (bin_raster_path, raster_id, aggregate_vector_id, 'nodata0')))
            stitch_queue.put(
                (bin_raster_path, (raster_id, aggregate_vector_id, 'nodata0')))

    except Exception:
        LOGGER.exception(
            'exception on feature_worker for %s %s %s' % (
                raster_id, aggregate_vector_id, feature_id))
        raise
    finally:
        task_graph.close()
        task_graph.join()


def extract_feature_checked(
        align_lock, vector_path, field_name, field_value, base_raster_path,
        target_vector_path, target_raster_path):
    """Extract single feature into separate vector and check for no error.

    Do not do a transform since it's all wgs84.

    Parameters:
        align_lock (multiprocessing.Lock): lock to only allow one align at
            a time.
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
    if not os.path.exists(base_raster_path):
        raise ValueError("%s does not exist" % base_raster_path)
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

            with align_lock:
                pygeoprocessing.align_and_resize_raster_stack(
                    [base_raster_path], [target_raster_path], ['near'],
                    base_raster_info['pixel_size'], 'intersection',
                    base_vector_path_list=[target_vector_path],
                    vector_mask_options={
                        'mask_vector_path': target_vector_path,
                    })
            return True
        except Exception:
            LOGGER.exception('exception when extracting %s %s %s' % (
                field_name, field_value, vector_path))
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


def get_value_list(vector_path, fieldname_id):
    """Returns a list of values of each features fieldname.

    Parameters:
        vector_path (str): path to vector that has fieldnames defined by the
            `fieldname_id_tuple`.
        fieldname_id (tuple): fieldname of value to retrieve.

    Returns:
        a list containing strings of the form '%s_%s_..._%s' for the number of
        %s there are elements in fieldname_id_tuple for the number of features
        in the vector in sorted order.

    """
    LOGGER.debug('getting field ')
    vector = gdal.OpenEx(vector_path, gdal.OF_VECTOR)
    layer = vector.GetLayer()
    value_list = set([feature.GetField(fieldname_id) for feature in layer])
    layer = None
    vector = None
    return list(sorted(value_list))


def gs_copy(gs_path, target_path):
    """Copy gs_path to target_path."""
    LOGGER.debug('about to gs copy %s to %s', gs_path, target_path)
    subprocess.run(
        'gsutil cp %s %s' % (gs_path, target_path), shell=True, check=True)
    LOGGER.debug('finished gs copy %s to %s', gs_path, target_path)


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
            CHURN_DIR, -1, 5.0)
    LOGGER.info('starting `main`')

    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)

    create_status_database_task = task_graph.add_task(
        func=create_status_database,
        args=(WORK_DATABASE_PATH,),
        target_path_list=[WORK_DATABASE_PATH],
        hash_target_files=False,
        task_name='initalize work database')
    LOGGER.debug('create work database')
    create_status_database_task.join()

    aggregate_vector_id_to_path = {}
    raster_id_to_path_map = {}
    for aggregate_vector_id, work_vector_dict in WORK_MAP.items():
        aggregate_vector_path = os.path.join(
            ECOSHARD_DIR, os.path.basename(work_vector_dict['vector_url']))
        aggregate_vector_id_to_path[aggregate_vector_id] = \
            aggregate_vector_path
        download_aggregate_vector_task = task_graph.add_task(
            func=ecoshard.download_url,
            args=(work_vector_dict['vector_url'], aggregate_vector_path),
            hash_target_files=False,
            target_path_list=[aggregate_vector_path],
            task_name='download aggregate vector path')
        work_vector_dict['vector_path'] = aggregate_vector_path

        raster_gs_path_list = subprocess.run(
            'gsutil ls -p ecoshard %s' % work_vector_dict[
                'raster_gs_pattern'],
            capture_output=True, shell=True, check=True)
        gs_path_list = [
            x.decode('utf-8') for x in raster_gs_path_list.stdout.splitlines()]
        raster_id_list = [
            os.path.basename(os.path.splitext(gs_path)[0])
            for gs_path in gs_path_list]

        download_aggregate_vector_task.join()
        feature_id_task = task_graph.add_task(
            func=get_value_list,
            args=(work_vector_dict['vector_path'],
                  work_vector_dict['fieldname_id']),
            ignore_path_list=[work_vector_dict['vector_path']],
            dependent_task_list=[download_aggregate_vector_task],
            task_name='fetch vector feature ids')

        feature_id_task.join()
        feature_id_list = feature_id_task.get()
        LOGGER.debug('field values list: %s', feature_id_list)

        # create any missing entries in the database if they don't exist:
        _execute_sqlite(
            'INSERT OR IGNORE INTO job_status('
            '   raster_id, aggregate_vector_id, fieldname_id, feature_id) '
            'VALUES (?, ?, ?, ?)', WORK_DATABASE_PATH,
            argument_list=[
                (raster_id, aggregate_vector_id,
                 work_vector_dict['fieldname_id'], feature_id)
                for raster_id, feature_id in
                itertools.product(
                    raster_id_list, feature_id_list + [GLOBAL_ID])],
            execute='many', mode='modify')

        LOGGER.debug('copy gs files')

        for gs_path in gs_path_list:
            LOGGER.debug('copy %s', gs_path)
            raster_id = os.path.basename(os.path.splitext(gs_path)[0])
            target_raster_path = os.path.join(
                CHURN_DIR, '%s_%s' % (
                    aggregate_vector_id, os.path.basename(gs_path)))
            raster_id_to_path_map[
                    (raster_id, aggregate_vector_id,
                     work_vector_dict['fieldname_id'])] = \
                target_raster_path
            _ = task_graph.add_task(
                func=gs_copy,
                args=(gs_path, target_raster_path),
                target_path_list=[target_raster_path],
                task_name='gs copy %s' % gs_path)
        LOGGER.debug('waiting for copy to finish')
        LOGGER.debug('gs copies are done')

        raster_id_agg_vector_tuples = _execute_sqlite(
            'SELECT raster_id, aggregate_vector_id, fieldname_id '
            'FROM job_status '
            'WHERE raster_id LIKE "realized_fwfish_distrib_catch%" '
            'GROUP BY raster_id, aggregate_vector_id',
            WORK_DATABASE_PATH, execute='execute', argument_list=[],
            fetch='all')
    task_graph.join()

    LOGGER.debug(
        'raster id agg vector tuples: %s', str(
            raster_id_agg_vector_tuples))

    raster_id_to_global_stitch_path_map = {}
    # This loop sets up empty rasters for stitching, one per raster type
    # / aggregate id / regular/nodata
    for raster_id, aggregate_vector_id, fieldname_id in \
            raster_id_agg_vector_tuples:
        # this loop will first do a "global" run, then a
        # per-feature id normalized one.
        for nodata_id in ['', 'nodata0']:
            global_stitch_raster_id = (
                '%s%s_by_%s_%s' % (
                    raster_id, nodata_id, aggregate_vector_id,
                    fieldname_id))
            global_stitch_raster_path = os.path.join(
                WORKSPACE_DIR, '%s.tif' % global_stitch_raster_id)
            LOGGER.debug(
                'make a global stitch raster: %s',
                global_stitch_raster_path)
            raster_id_to_global_stitch_path_map[
                (raster_id, aggregate_vector_id, nodata_id)] = (
                    global_stitch_raster_path)
            LOGGER.debug(
                'get info from: %s', raster_id_to_path_map[
                    (raster_id, aggregate_vector_id, fieldname_id)])
            raster_info = pygeoprocessing.get_raster_info(
                raster_id_to_path_map[
                    (raster_id, aggregate_vector_id, fieldname_id)])
            LOGGER.debug('info: %s', raster_info)
            task_graph.add_task(
                func=new_raster_from_base,
                args=(
                    raster_id_to_path_map[
                        (raster_id, aggregate_vector_id, fieldname_id)],
                    global_stitch_raster_id, WORKSPACE_DIR,
                    raster_info['datatype'], raster_info['nodata'][0]),
                hash_target_files=False,
                target_path_list=[global_stitch_raster_path],
                task_name='make empty stitch raster for %s%s' % (
                    raster_id, nodata_id))

    task_graph.close()
    task_graph.join()

    raster_vector_feature_tuples = _execute_sqlite(
        '''
        SELECT raster_id, aggregate_vector_id, fieldname_id, feature_id
        FROM job_status
        WHERE percentile0_list is NULL
        ''', WORK_DATABASE_PATH, execute='execute', argument_list=[],
        fetch='all')

    work_queue = multiprocessing.Queue()
    for raster_id, aggregate_vector_id, fieldname_id, feature_id in \
            raster_vector_feature_tuples:
        if feature_id in SKIP_THESE_FEATURE_IDS:
            continue
        LOGGER.debug(
            'putting %s %s %s to work',
            raster_id, aggregate_vector_id, feature_id)
        work_queue.put(
            (raster_id, aggregate_vector_id, feature_id, fieldname_id))

    work_queue.put('STOP')

    stitch_queue = multiprocessing.Queue()
    m_manager = multiprocessing.Manager()
    align_lock = m_manager.Lock()
    worker_list = []
    for worker_id in range(NCPUS):
        country_worker_process = multiprocessing.Process(
            target=feature_worker,
            args=(
                work_queue, align_lock, aggregate_vector_id_to_path,
                raster_id_to_path_map, stitch_queue),
            name='%d' % worker_id)
        country_worker_process.start()
        worker_list.append(country_worker_process)

    raster_id_lock_map = {
        raster_id_nodata_id_tuple: multiprocessing.Lock()
        for raster_id_nodata_id_tuple in raster_id_to_global_stitch_path_map
    }

    # TODO:
    # stitch_worker_list = []
    # for worker_id in range(NCPUS):
    #     stitch_worker_process = multiprocessing.Process(
    #         target=stitch_worker,
    #         args=(stitch_queue, raster_id_to_global_stitch_path_map,
    #               raster_id_lock_map),
    #         name='stitch worker %s' % worker_id)
    #     stitch_worker_process.start()
    #     stitch_worker_list.append(stitch_worker_process)

    LOGGER.debug('wait for workers to stop')
    for process in worker_list:
        process.join()

    LOGGER.debug('workers stopped')
    stitch_queue.put('STOP')

    LOGGER.debug('building histogram/cdf')
    for (raster_id, _, _), raster_path in raster_id_to_path_map.items():

        LOGGER.debug('building csv for %s %s', raster_id, raster_path)
        result = _execute_sqlite(
            '''
            SELECT
              feature_id, percentile_list, percentile0_list, cdf, cdfnodata0
            FROM job_status
            WHERE raster_id=?;
            ''',
            WORK_DATABASE_PATH, execute='execute', argument_list=[raster_id],
            fetch='all')

        percentile_map = {
            feature_id: (
                pickle.loads(percentile_list),
                pickle.loads(percentile0_list),
                pickle.loads(cdf),
                pickle.loads(cdfnodata0))
            for (feature_id, percentile_list, percentile0_list,
                 cdf, cdfnodata0) in result
            if None not in (
                feature_id, percentile_list, percentile0_list, cdf, cdfnodata0)
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
            for feature_id in ['_GLOBAL'] + \
                    sorted(set(percentile_map)-set(['_GLOBAL'])):
                csv_cdf_file.write(
                    '\n%s,' % feature_id +
                    ','.join(reversed([
                        str(x) for x in percentile_map[feature_id][2]])))

        with open(csv_nodata0_cdf_path, 'w') as csv_cdf_nodata0_file:
            csv_cdf_nodata0_file.write('%s cdfs' % raster_id)
            csv_cdf_nodata0_file.write(
                '\ncountry,' +
                ','.join([str(x) for x in PERCENTILE_LIST]))
            # first do the whole world
            for feature_id in ['_GLOBAL'] + \
                    sorted(set(percentile_map)-set(['_GLOBAL'])):
                csv_cdf_nodata0_file.write(
                    '\n%s,' % feature_id +
                    ','.join(reversed([
                        str(x) for x in percentile_map[feature_id][3]])))

        with open(csv_percentile_path, 'w') as csv_percentile_file:
            csv_percentile_file.write('%s percentiles' % raster_id)
            csv_percentile_file.write(
                '\ncountry,' +
                ','.join([str(x) for x in PERCENTILE_LIST]))
            # first do the whole world
            for feature_id in ['_GLOBAL'] + \
                    sorted(set(percentile_map)-set(['_GLOBAL'])):
                csv_percentile_file.write(
                    '\n%s,' % feature_id +
                    ','.join([str(x) for x in percentile_map[feature_id][0]]))

        with open(csv_nodata0_percentile_path, 'w') as \
                csv_nodata0_percentile_file:
            csv_nodata0_percentile_file.write('%s percentiles' % raster_id)
            csv_nodata0_percentile_file.write(
                '\ncountry,' +
                ','.join([str(x) for x in PERCENTILE_LIST]))
            # first do the whole world
            for feature_id in ['_GLOBAL'] + \
                    sorted(set(percentile_map)-set(['_GLOBAL'])):
                csv_nodata0_percentile_file.write(
                    '\n%s,' % feature_id +
                    ','.join([str(x) for x in percentile_map[feature_id][1]]))

    LOGGER.debug('wait for stitch to stop')
    # TODO:
    # for stitch_worker_process in stitch_worker_list:
    #     stitch_worker_process.join()
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
        raster_id_aggregate_id_lock_map):
    """Stitch incoming country rasters into global raster.

    Parameters:
        stitch_queue (queue): payloads come in as an alert that a sub raster
            is ready for stitching into the global raster. Payloads are of the
            form (bin_raster_path,(raster_id, aggregate_vector_id, nodataflag))
            is indexed into
            `raster_id_to_global_stitch_path_map[
                (raster_id, aggregate_id, nodata_flag)]`.
        raster_id_to_global_stitch_path_map (dict): dictionary indexed by
            raster id, aggregate id, and nodata flag tuple to the global
            raster.
        raster_id_aggregate_id_lock_map (dict): mapping raster ids to
            multiprocessing Locks so we don't edit more than one raster at a
            time.

    """
    try:
        while True:
            payload = stitch_queue.get()
            if payload == 'STOP':
                LOGGER.info('stopping stitch_worker')
                stitch_queue.put('STOP')
                break
            local_tile_raster_path, raster_aggregate_nodata_id_tuple = payload
            global_stitch_raster_path = raster_id_to_global_stitch_path_map[
                raster_aggregate_nodata_id_tuple]

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
            with raster_id_aggregate_id_lock_map[
                    raster_aggregate_nodata_id_tuple]:
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
    LOGGER.debug('making new raster %s', target_raster_path)
    pygeoprocessing.new_raster_from_base(
        base_raster, target_raster_path,
        target_datatype, [target_nodata])
    LOGGER.debug('done making new raster %s', target_raster_path)


@retrying.retry(wait_exponential_multiplier=1000, wait_exponential_max=5000)
def _execute_sqlite(
        sqlite_command, database_path, argument_list=None,
        mode='read_only', execute='execute', fetch=None):
    """Execute SQLite command and attempt retries on a failure.

    Parameters:
        sqlite_command (str): a well formatted SQLite command.
        database_path (str): path to the SQLite database to operate on.
        argument_list (list): `execute == 'execute` then this list is passed to
            the internal sqlite3 `execute` call.
        mode (str): must be either 'read_only' or 'modify'.
        execute (str): must be either 'execute', 'many', or 'script'.
        fetch (str): if not `None` can be either 'all' or 'one'.
            If not None the result of a fetch will be returned by this
            function.

    Returns:
        result of fetch if `fetch` is not None.

    """
    cursor = None
    connection = None
    try:
        if mode == 'read_only':
            ro_uri = r'%s?mode=ro' % pathlib.Path(
                os.path.abspath(database_path)).as_uri()
            LOGGER.debug(
                '%s exists: %s', ro_uri, os.path.exists(os.path.abspath(
                    database_path)))
            connection = sqlite3.connect(ro_uri, uri=True)
        elif mode == 'modify':
            connection = sqlite3.connect(database_path)
        else:
            raise ValueError('Unknown mode: %s' % mode)

        if execute == 'execute':
            cursor = connection.execute(sqlite_command, argument_list)
        elif execute == 'many':
            cursor = connection.executemany(sqlite_command, argument_list)
        elif execute == 'script':
            cursor = connection.executescript(sqlite_command)
        else:
            raise ValueError('Unknown execute mode: %s' % execute)

        result = None
        payload = None
        if fetch == 'all':
            payload = (cursor.fetchall())
        elif fetch == 'one':
            payload = (cursor.fetchone())
        elif fetch is not None:
            raise ValueError('Unknown fetch mode: %s' % fetch)
        if payload is not None:
            result = list(payload)
        cursor.close()
        connection.commit()
        connection.close()
        return result
    except Exception:
        LOGGER.exception('Exception on _execute_sqlite: %s', sqlite_command)
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.commit()
            connection.close()
        raise


if __name__ == '__main__':
    try:
        main()
    except Exception:
        LOGGER.exception('error on main!')
