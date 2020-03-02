"""Count non-zero non-nodata pixels in raster, get percentile values, and sum above each percentile to build the CDF."""
import datetime
import ecoshard
import logging
import logging.handlers
import multiprocessing
import os
import sqlite3
import subprocess
import sys
import threading

import matplotlib.pyplot
import numpy
import scipy.interpolate
import pygeoprocessing
from osgeo import gdal
from osgeo import osr
from osgeo import ogr
import taskgraph

gdal.SetCacheMax(2**30)

BUCKET_PATTERN = 'gs://shared-with-users/realized_services/*.tif'
WORKSPACE_DIR = 'cdf_by_country'
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')
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
COUNTRY_WORKSPACES = os.path.join(WORKSPACE_DIR, 'country_workspaces')

PERCENTILE_LIST = list(range(0, 101, 5))

WORK_DATABASE_PATH = os.path.join(CHURN_DIR, 'work_status.sqlite3')
WORK_DATABSE_INIT_TOKEN_PATH = os.path.join(
    CHURN_DIR, '%s.INITALIZED' % os.path.basename(WORK_DATABASE_PATH))


def main(work_queue):
    """Entry point.

    Parameters:
        work_queue (queue): Country gs:// paths should be pushed here for
            further processing.

    """
    LOGGER.info('starting `main`')
    for dir_path in [WORKSPACE_DIR, CHURN_DIR, COUNTRY_WORKSPACES]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    task_graph = taskgraph.TaskGraph(CHURN_DIR, 0, 5.0)
    world_borders_path = os.path.join(
        WORKSPACE_DIR, os.path.basename(WORLD_BORDERS_URL))
    download_world_borders_task = task_graph.add_task(
        func=ecoshard.download_url,
        args=(WORLD_BORDERS_URL, world_borders_path),
        target_path_list=[world_borders_path],
        task_name='download world borders')

    download_world_borders_task.join()
    country_id_list = get_value_list(world_borders_path, COUNTRY_ID_FIELDNAME)

    LOGGER.debug(country_id_list)
    work_queue.put('STOP')
    sys.exit(0)

    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)

    result = subprocess.run(
        'gsutil ls -p ecoshard %s' % BUCKET_PATTERN, capture_output=True,
        shell=True, check=True)
    country_raster_path_list = []
    gs_path_list = [x.decode('utf-8') for x in result.stdout.splitlines()]
    raster_id_list = [
        os.path.basename(os.path.splitext(gs_path)[0])
        for gs_path in gs_path_list]

    create_status_database_task = task_graph.add_task(
        func=create_status_database,
        args=(WORK_DATABASE_PATH, raster_id_list, WORK_DATABSE_INIT_TOKEN_PATH),
        target_path_list=[WORK_DATABSE_INIT_TOKEN_PATH],
        ignore_path_list=[WORK_DATABASE_PATH],
        task_name='initalize work database')
    create_status_database_task.join()
    download_world_borders_task.join()

    for gs_path in gs_path_list:
        print(gs_path)
        raster_id = os.path.basename(os.path.splitext(gs_path)[0])
        raster_path = os.path.join(CHURN_DIR, os.path.basename(gs_path))
        print('copying %s to %s' % (gs_path, raster_path))
        subprocess.run(
            'gsutil cp %s %s' % (gs_path, raster_path), shell=True, check=True)

        work_queue.put((raster_id, raster_path))

    task_graph.join()
    task_graph.close()
    del task_graph

    work_queue = multiprocessing.Queue()
    global_lock = multiprocessing.Lock()
    worker_list = []
    for worker_id in range(multiprocessing.cpu_count()):
        country_worker_process = multiprocessing.Process(
            target=process_country_worker,
            args=(work_queue, global_lock),
            name='%d' % worker_id)
        country_worker_process.start()
        worker_list.append(country_worker_process)

    for (country_name, country_threshold_table_path,
            percentile_per_country_filename, raster_id,
            country_raster_path) in country_raster_path_list:
        work_queue.put(
            (country_name, country_threshold_table_path,
             percentile_per_country_filename, raster_id,
             country_raster_path))

    work_queue.put('STOP')
    for process in country_worker_process:
        process.join()


def create_status_database(database_path, raster_id_list, complete_token_path):
    """Create a runtime status database if it doesn't exist.

    Parameters:
        database_path (str): path to database to create.
        raster_id_list (list): list of raster id strings that will be monitored
            for completion.
        complete_token_path (str): path to a text file that will be created
            by this function written with the timestamp when it finishes.

    Returns:
        None.

    """
    LOGGER.debug('launching create_status_database')
    create_database_sql = (
        """
        CREATE TABLE job_status (
            raster_id TEXT NOT NULL,
            complete INT NOT NULL);
        """)
    if os.path.exists(database_path):
        os.remove(database_path)
    connection = sqlite3.connect(database_path)
    cursor = connection.cursor()
    cursor.executescript(create_database_sql)

    insert_query = (
        'INSERT INTO job_status(raster_id, complete) '
        'VALUES (?, 0)')

    cursor.executemany(insert_query, raster_id_list)
    with open(complete_token_path, 'w') as complete_token_file:
        complete_token_file.write(str(datetime.datetime.now()))
    connection.commit()

    with open(complete_token_path, 'w') as token_file:
        token_file.write(str(datetime.datetime.now()))


def process_country_worker(work_queue, global_lock):
    """Process work queue."""
    while True:
        payload = work_queue.get()
        if payload == 'STOP':
            work_queue.put('STOP')
            break
        process_country_percentile(global_lock, *payload)


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


def extract_feature(
        vector_path, feature_id, projection_wkt, target_vector_path,
        target_complete_token_path):
    """Make a local projection of a single feature in a vector.

    Parameters:
        vector_path (str): base vector in WGS84 coordinates.
        feature_id (int): FID for the feature to extract.
        projection_wkt (str): projection wkt code to project feature to.
        target_gpkg_vector_path (str): path to new GPKG vector that will
            contain only that feature.
        target_complete_token_path (str): path to a file that is created if
             the function successfully completes.

    Returns:
        None.

    """
    base_vector = gdal.OpenEx(vector_path, gdal.OF_VECTOR)
    base_layer = base_vector.GetLayer()
    feature = base_layer.GetFeature(feature_id)
    geom = feature.GetGeometryRef()

    epsg_srs = osr.SpatialReference()
    epsg_srs.ImportFromWkt(projection_wkt)

    base_srs = base_layer.GetSpatialRef()
    base_to_utm = osr.CoordinateTransformation(base_srs, epsg_srs)

    # clip out watershed to its own file
    # create a new shapefile
    if os.path.exists(target_vector_path):
        os.remove(target_vector_path)
    driver = ogr.GetDriverByName('GPKG')
    target_vector = driver.CreateDataSource(
        target_vector_path)
    target_layer = target_vector.CreateLayer(
        os.path.splitext(os.path.basename(target_vector_path))[0],
        epsg_srs, ogr.wkbMultiPolygon)
    layer_defn = target_layer.GetLayerDefn()
    feature_geometry = geom.Clone()
    base_feature = ogr.Feature(layer_defn)
    feature_geometry.Transform(base_to_utm)
    base_feature.SetGeometry(feature_geometry)
    target_layer.CreateFeature(base_feature)
    target_layer.SyncToDisk()
    geom = None
    feature_geometry = None
    base_feature = None
    target_layer = None
    target_vector = None
    base_layer = None
    base_vector = None
    with open(target_complete_token_path, 'w') as token_file:
        token_file.write(str(datetime.datetime.now()))


def raster_worker(work_queue, churn_dir):
    """Process `work_queue` for gs:// paths or 'STOP'.

    Parameters:
        work_queue (queue): contains (raster_id, raster_uri) pairs or 'STOP'
            sentinel. `raster_uri` can be in the format of `gs://`, `http`, or
            a local file path. It must not be a pattern. If a cloud path it
            will attempt to download that file first.

    Returns:
        None when reciving a 'STOP' in the work_queue.

    """
    LOGGER.debug(
        'starting raster worker %s', threading.current_thread())

    world_borders_path = os.path.join(
        WORKSPACE_DIR, os.path.basename(WORLD_BORDERS_URL))
    world_borders_vector = gdal.OpenEx(world_borders_path, gdal.OF_VECTOR)
    world_borders_layer = world_borders_vector.GetLayer()

    while True:
        payload = work_queue.get()
        if payload == 'STOP':
            LOGGER.info('stopping %s', threading.current_thread())
            work_queue.put('STOP')
            return
        raster_id, raster_uri = payload
        if not os.path.exists(raster_uri):
            raster_path = os.path.join(churn_dir, os.path.basename(raster_uri))
            # note this will only re-download a file if it doesn't exist on disk
            # this does not mean the file has not changed!
            if not os.path.exists(raster_path):
                if raster_uri.startswith('gs://'):
                    copy_from_gs(raster_uri, raster_path)
                elif raster_uri.startswith('http'):
                    ecoshard.download_url(raster_uri, raster_path)
                else:
                    raise ValueError(
                        '%s is not a valid URI or filepath found on disk' %
                        raster_uri)

        # make a zeros removed raster

        raster_info = pygeoprocessing.get_raster_info(raster_path)
        country_threshold_table_path = os.path.join(
            WORKSPACE_DIR, 'country_threshold_%s.csv' % raster_id)
        country_threshold_table_file = open(country_threshold_table_path, 'w')
        country_threshold_table_file.write(
            'country,percentile at 90% max,pixel count\n')
        country_threshold_table_file.close()
        percentile_per_country_filename = '%s_percentile.csv' % raster_id
        percentile_per_country_file = open(
            percentile_per_country_filename, 'w')
        percentile_per_country_file.write('country name,' + ','.join(
            [str(x) for x in PERCENTILE_LIST]) + '\n')
        percentile_per_country_file.close()

        for world_border_feature in world_borders_layer:
            country_name = world_border_feature.GetField('nev_name')
            LOGGER.debug(country_name)
            country_workspace = os.path.join(COUNTRY_WORKSPACES, country_name)
            try:
                os.makedirs(country_workspace)
            except OSError:
                pass

            country_vector = os.path.join(
                country_workspace, '%s.gpkg' % country_name)
            country_vector_complete_token = os.path.join(
                country_workspace, '%s.COMPLETE' % country_name)
            extract_task = task_graph.add_task(
                func=extract_feature,
                args=(
                    world_borders_path, world_border_feature.GetFID(),
                    wgs84_srs.ExportToWkt(), country_vector,
                    country_vector_complete_token),
                target_path_list=[country_vector_complete_token],
                task_name='exctract %s' % country_name)

            country_raster_path = os.path.join(country_workspace, '%s_%s' % (
                country_name, os.path.basename(raster_path)))

            country_vector_info = pygeoprocessing.get_vector_info(
                country_vector)
            task_graph.add_task(
                func=pygeoprocessing.warp_raster,
                args=(
                    raster_path, raster_info['pixel_size'],
                    country_raster_path, 'near'),
                kwargs={
                    'target_bb': country_vector_info['bounding_box'],
                    'vector_mask_options': {
                        'mask_vector_path': country_vector},
                    'working_dir': country_workspace},
                target_path_list=[country_raster_path],
                dependent_task_list=[extract_task],
                task_name='warp %s' % country_name)
            country_raster_path_list.append(
                (country_name, country_threshold_table_path,
                 percentile_per_country_filename, raster_id,
                 country_raster_path))


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


if __name__ == '__main__':
    LOGGER.info('Initalizing Workers')
    N_WORKERS = multiprocessing.cpu_count()
    work_queue = multiprocessing.Queue()
    for worker_id in range(N_WORKERS):
        raster_worker_process = threading.Thread(
            target=raster_worker,
            args=(work_queue, CHURN_DIR),
            name='worker-%d' % worker_id)
        #raster_worker_process.start()
    main(work_queue)
