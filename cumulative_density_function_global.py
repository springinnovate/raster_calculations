"""Count non-zero non-nodata pixels in raster, get percentile values, and sum above each percentile to build the CDF."""
import datetime
import ecoshard
import logging
import os
import pickle
import shutil
import sys
import tempfile

from osgeo import gdal
from osgeo import ogr
from osgeo import osr
import matplotlib.pyplot
import numpy
import pygeoprocessing
import scipy.interpolate
import taskgraph

gdal.SetCacheMax(2**30)

#RASTER_PATH = 'agbc2010_MgCha_x10_masked.tif'
#RASTER_PATH = r"C:\Users\Rich\Downloads\agbc2010_MgCha_x10.tif"
RASTER_URL = 'https://storage.googleapis.com/critical-natural-capital-ecoshards/agbc2010_MgCha_x10_md5_7c945a8b05f65c3ac1e84e4a20e2f114.tif'
WORKSPACE_DIR = 'cdf_global_workspace'
#WORKSPACE_DIR = 'cdf_by_country'
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
logging.getLogger('matplotlib').setLevel(logging.ERROR)

#WORLD_BORDERS_URL = 'https://storage.googleapis.com/critical-natural-capital-ecoshards/land_area_md5_067cd6de8987123cecfb36d7b49b8b40.gpkg'
#WORLD_BORDERS_URL = 'https://storage.googleapis.com/critical-natural-capital-ecoshards/countries_iso3_md5_6fb2431e911401992e6e56ddf0a9bcda.gpkg'
COUNTRY_WORKSPACES = os.path.join(WORKSPACE_DIR, 'country_workspaces')

GTIFF_CREATION_TUPLE_OPTIONS = ('GTIFF', (
    'TILED=YES', 'BIGTIFF=YES', 'COMPRESS=ZSTD',
    'BLOCKXSIZE=256', 'BLOCKYSIZE=256'))
PERCENTILE_LIST = list(range(0, 101, 5))


def calculate_percentiles(
        raster_path, percentile_list, target_percentile_pickle_path):
    """Calculate percentiles and save to a pickle file.

    Parameters:
        raster_path (str): path to raster.
        percentile_list (list): list of increasing order percentile thresholds
            between the ranges 0-100.
        target_percentile_pickle_path (str): the result of the percentile
            function will be saved in a list that is pickled in this file.

    Returns:
        None.

    """
    working_dir = os.path.dirname(target_percentile_pickle_path)
    heapfile_dir = tempfile.mkdtemp(dir=working_dir)
    percentile_values = pygeoprocessing.raster_band_percentile(
        (raster_path, 1), heapfile_dir, percentile_list)
    with open(target_percentile_pickle_path, 'wb') as pickle_file:
        pickle.dump(percentile_values, pickle_file)
    shutil.rmtree(heapfile_dir)


def main():
    """Entry point."""
    #for dir_path in [WORKSPACE_DIR, COUNTRY_WORKSPACES]:
    #    try:
    #        os.makedirs(dir_path)
    #    except OSError:
    #        pass

    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, -1, 5.0)
    world_borders_path = os.path.join(
        WORKSPACE_DIR, os.path.basename(WORLD_BORDERS_URL))
    download_task = task_graph.add_task(
        func=ecoshard.download_url,
        args=(WORLD_BORDERS_URL, world_borders_path),
        target_path_list=[world_borders_path],
        task_name='download world borders')

    #download_task.join()

    #world_borders_vector = gdal.OpenEx(world_borders_path, gdal.OF_VECTOR)
    #world_borders_layer = world_borders_vector.GetLayer()

    #wgs84_srs = osr.SpatialReference()
    #wgs84_srs.ImportFromEPSG(4326)

    # mask out everything that's not a country
    masked_raster_path = os.path.join(
        WORKSPACE_DIR, '%s_masked.%s' % os.path.splitext(
            os.path.basename(RASTER_PATH)))
    # we need to define this because otherwise no nodata value is defined
    mask_nodata = -1
    mask_task = task_graph.add_task(
        func=pygeoprocessing.mask_raster,
        args=(
            (RASTER_PATH, 1), world_borders_path, masked_raster_path),
        kwargs={
            'raster_driver_creation_tuple': GTIFF_CREATION_TUPLE_OPTIONS,
            'target_mask_value': mask_nodata,
        },
        target_path_list=[masked_raster_path],
        task_name='mask raster')

    raster_info = pygeoprocessing.get_raster_info(RASTER_PATH)
    country_name = "Global"

    country_threshold_table_path = os.path.join(
        WORKSPACE_DIR, 'country_threshold.csv')
    country_threshold_table_file = open(country_threshold_table_path, 'w')
    country_threshold_table_file.write('country,percentile at 90% max,pixel count\n')

    target_percentile_pickle_path = os.path.join(
        WORKSPACE_DIR, '%s.pkl' % (
            os.path.basename(os.path.splitext(RASTER_PATH)[0])))
    calculate_percentiles_task = task_graph.add_task(
        func=calculate_percentiles,
        args=(
            RASTER_PATH, PERCENTILE_LIST, target_percentile_pickle_path),
        target_path_list=[target_percentile_pickle_path],
        dependent_task_list=[mask_task],
        task_name='calculate percentiles')
    calculate_percentiles_task.join()
    with open(target_percentile_pickle_path, 'rb') as pickle_file:
        percentile_values = pickle.load(pickle_file)
    LOGGER.debug(
        "len percentile_values: %d len PERCENTILE_LIST: %d",
        len(percentile_values), len(PERCENTILE_LIST))

    cdf_array = [0.0] * len(percentile_values)

    raster_info = pygeoprocessing.get_raster_info(RASTER_PATH)
    nodata = raster_info['nodata'][0]
    valid_pixel_count = 0
    total_pixel_count = 0
    total_pixels = (
        raster_info['raster_size'][0] * raster_info['raster_size'][1])
    for _, data_block in pygeoprocessing.iterblocks(
            (RASTER_PATH, 1), largest_block=2**28):
        nodata_mask = ~numpy.isclose(data_block, nodata)
        nonzero_count = numpy.count_nonzero(nodata_mask)
        if nonzero_count == 0:
            continue
        valid_pixel_count += numpy.count_nonzero(nodata_mask)
        for index, percentile_value in enumerate(percentile_values):
            cdf_array[index] += numpy.sum((data_block[
                nodata_mask & (data_block >= percentile_value)]).astype(
                    numpy.float32))
        total_pixel_count += data_block.size
        LOGGER.debug('%.2f%% complete', (100.0*total_pixel_count)/total_pixels)
        LOGGER.debug('current cdf array: %s', cdf_array)
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

    ax.plot([0, 100], [threshold_limit, threshold_limit], 'k:', linewidth=2)
    ax.plot([cdf_threshold, cdf_threshold], [cdf_array[0], cdf_array[-1]], 'k:', linewidth=2)

    ax.grid(True, linestyle='-.')
    ax.set_title(
        '%s CDF. 90%% max at %.2f and %.2f%%\nn=%d' % (country_name, threshold_limit, cdf_threshold, valid_pixel_count))
    ax.set_ylabel('Sum of %s up to 100-percentile' % os.path.basename(RASTER_PATH))
    ax.set_ylabel('100-percentile')
    ax.tick_params(labelcolor='r', labelsize='medium', width=3)
    matplotlib.pyplot.autoscale(enable=True, tight=True)
    matplotlib.pyplot.savefig(
        os.path.join(COUNTRY_WORKSPACES, '%s_cdf.png' % country_name))
    country_threshold_table_file.write(
        '%s, %f, %d\n' % (country_name, cdf_threshold, valid_pixel_count))
    country_threshold_table_file.flush()
    country_threshold_table_file.close()

    return

    for world_border_feature in world_borders_layer:
        country_name = world_border_feature.GetField('nev_name')
        country_name= country_name.replace('.','')
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
        extract_feature(
            world_borders_path, world_border_feature.GetFID(),
            wgs84_srs.ExportToWkt(), country_vector,
            country_vector_complete_token)

        country_raster_path = os.path.join(country_workspace, '%s_%s' % (
            country_name, os.path.basename(RASTER_PATH)))

        country_vector_info = pygeoprocessing.get_vector_info(country_vector)
        pygeoprocessing.warp_raster(
            RASTER_PATH, raster_info['pixel_size'], country_raster_path,
            'near', target_bb=country_vector_info['bounding_box'],
            vector_mask_options={'mask_vector_path': country_vector},
            working_dir=country_workspace)

        percentile_values = pygeoprocessing.raster_band_percentile(
            (country_raster_path, 1), country_workspace, PERCENTILE_LIST)
        if len(percentile_values) != len(PERCENTILE_LIST):
            continue
        LOGGER.debug(
            "len percentile_values: %d len PERCENTILE_LIST: %d",
            len(percentile_values), len(PERCENTILE_LIST))

        cdf_array = [0.0] * len(percentile_values)

        nodata = pygeoprocessing.get_raster_info(
            country_raster_path)['nodata'][0]
        valid_pixel_count = 0
        for _, data_block in pygeoprocessing.iterblocks(
                (country_raster_path, 1)):
            nodata_mask = ~numpy.isclose(data_block, nodata)
            valid_pixel_count += numpy.count_nonzero(nodata_mask)
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

        ax.plot([0, 100], [threshold_limit, threshold_limit], 'k:', linewidth=2)
        ax.plot([cdf_threshold, cdf_threshold], [cdf_array[0], cdf_array[-1]], 'k:', linewidth=2)

        ax.grid(True, linestyle='-.')
        ax.set_title(
            '%s CDF. 90%% max at %.2f and %.2f%%\nn=%d' % (country_name, threshold_limit, cdf_threshold, valid_pixel_count))
        ax.set_ylabel('Sum of %s up to 100-percentile' % os.path.basename(RASTER_PATH))
        ax.set_ylabel('100-percentile')
        ax.tick_params(labelcolor='r', labelsize='medium', width=3)
        matplotlib.pyplot.autoscale(enable=True, tight=True)
        matplotlib.pyplot.savefig(
            os.path.join(COUNTRY_WORKSPACES, '%s_cdf.png' % country_name))
        country_threshold_table_file.write(
            '%s, %f, %d\n' % (country_name, cdf_threshold, valid_pixel_count))
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
    layername = (os.path.splitext(os.path.basename(target_vector_path))[0])
    target_layer = target_vector.CreateLayer(
        layername, epsg_srs, ogr.wkbMultiPolygon)
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

if __name__ == '__main__':
    main()