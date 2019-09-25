"""
Normalize a raster by percentile in a polygon.

    * given a polygon make a local stats list there.
    * option to clamp 0..1
"""
import sys
import pickle
import shutil
import os
import tempfile
import logging

import ecoshard
import numpy
import shapely.wkb
from osgeo import gdal
from osgeo import ogr
import taskgraph
import pygeoprocessing
import raster_calculations_core

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.DEBUG)


def normalize_by_polygon(
        raster_path, vector_path, percentile, clamp_range, workspace_dir,
        target_path):
    """Normalize a raster locally by regions defined by vector.

    Parameters:
        raster_path (str): path to base raster to aggregate over.
        vector_path (str): path to a vector that defines local regions to
            normalize over. Any pixels outside of these polygons will be set
            to nodata.
        percentile (float): a number in the range [0, 100] that is used to
            normalize the local regions defined by `vector_path`. This number
            will be used to calculate the percentile in each region separately
        clamp_range (list or tuple): a min/max range to clamp the normalized
            result by.
        workspace_dir (str): path to a workspace to create and keep
            intermediate files.

    Returns:
        None.

    """
    base_dir = os.path.dirname(target_path)
    for dir_path in [base_dir, workspace_dir]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    vector = ogr.Open(vector_path)
    layer = vector.GetLayer()
    fid_to_percentile_pickle_path = {}
    for feature in layer:
        # clip the original layer and then mask it
        fid = feature.GetFID()
        feature_mask_path = os.path.join(
            workspace_dir, '%d_mask.tif' % fid)
        mask_raster_task = TASK_GRAPH.add_task(
            func=clip_and_mask_raster,
            args=(raster_path, vector_path, fid, feature_mask_path),
            target_path_list=[feature_mask_path],
            task_name='mask feature %d' % fid)
        percentile_pickle_path = os.path.join(
            workspace_dir, '%d_%d.pickle' % (fid, percentile))
        # _ = TASK_GRAPH.add_task(
        #     func=calculate_percentile,
        #     args=(feature_mask_path, [percentile], base_dir,
        #           percentile_pickle_path),
        #     target_path_list=[percentile_pickle_path],
        #     dependent_task_list=[mask_raster_task],
        #     task_name='calculating %s' % percentile_pickle_path)
        fid_to_percentile_pickle_path[fid] = percentile_pickle_path
        feature = None

    return

    local_vector_path = os.path.join(workspace_dir, 'local_vector.gpkg')
    gpkg_driver = ogr.GetDriverByName('GPKG')
    local_vector = gpkg_driver.CopyDataSource(vector, local_vector_path)
    vector = None
    layer = None
    local_layer = local_vector.GetLayer()
    local_layer.CreateField(ogr.FieldDefn('norm_val', ogr.OFTReal))

    global_norm_value_raster_path = os.path.join(
        workspace_dir, 'global_norm_values.tif')
    pygeoprocessing.new_raster_from_base(
        raster_path, global_norm_value_raster_path, gdal.GDT_Float32,
        [-1], raster_driver_creation_tuple=(
            'GTIFF', (
                'TILED=YES', 'BIGTIFF=YES', 'COMPRESS=ZSTD',
                'BLOCKXSIZE=256', 'BLOCKYSIZE=256', 'SPARSE_OK=TRUE')))

    TASK_GRAPH.join()
    for fid, pickle_path in fid_to_percentile_pickle_path.items():
        feature = local_layer.GetFeature(fid)
        with open(pickle_path, 'rb') as pickle_file:
            percentile_list = pickle.load(pickle_file)
        feature.SetField('norm_val', percentile_list[0])
        local_layer.SetFeature(feature)
        feature = None
    local_layer = None
    local_vector = None

    pygeoprocessing.rasterize(
        local_vector_path, global_norm_value_raster_path,
        option_list=['ATTRIBUTE=norm_val'])

    raster_nodata = pygeoprocessing.get_raster_info(raster_path)['nodata'][0]
    pygeoprocessing.raster_calculator(
        [(raster_path, 1), (global_norm_value_raster_path, 1),
         (clamp_range, 'raw'), (raster_nodata, 'raw'), (-1, 'raw'),
         (-1, 'raw')], divide_op,
        target_path, gdal.GDT_Float32, -1)


def divide_op(
        raster_a, raster_b, clamp_range, a_nodata, b_nodata, target_nodata):
    """Divide a by b."""
    result = numpy.empty(raster_a.shape)
    result[:] = target_nodata
    valid_mask = (
        (~numpy.isclose(raster_a, a_nodata)) &
        (~numpy.isclose(raster_b, b_nodata)) &
        (raster_b != 0.0))
    result[valid_mask] = raster_a[valid_mask] / raster_b[valid_mask]
    result[result[valid_mask] <= clamp_range[0]] = clamp_range[0]
    result[result[valid_mask] >= clamp_range[1]] = clamp_range[1]
    return result


def clip_and_mask_raster(
        raster_path, vector_path, fid, target_mask_path):
    """Clip raster to feature and then mask by geometry.

    Parameters:
        raster_path (str): path to raster to clip.
        vector_path (str): path to vector that contains feature `fid`.
        fid (int): feature ID to use as the clipping feature.
        target_mask_path (str): raster is created as 0, 1, bounds extends the
            envelope of the feature and is 1 where it overlaps.

    Returns:
        None.

    """
    vector = gdal.OpenEx(vector_path, gdal.OF_VECTOR)
    layer = vector.GetLayer()
    feature = layer.GetFeature(fid)
    geometry_ref = feature.GetGeometryRef()
    geometry = shapely.wkb.loads(geometry_ref.ExportToWkb())
    base_dir = os.path.dirname(target_mask_path)
    pixel_size = pygeoprocessing.get_raster_info(raster_path)['pixel_size']
    fh, target_clipped_path = tempfile.mkstemp(
        suffix='.tif', prefix='clipped', dir=base_dir)
    os.close(fh)
    LOGGER.debug('%s: %s', vector_path, str(geometry.bounds))
    pygeoprocessing.warp_raster(
        raster_path, pixel_size, target_clipped_path,
        'near', target_bb=geometry.bounds)
    pygeoprocessing.mask_raster(
        (target_clipped_path, 1), vector_path, target_mask_path,
        where_clause='FID=%d' % fid)
    os.remove(target_clipped_path)


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
            a list of percentile threshold values in the same position in
            `percentile_list`.

    Returns:
        None.

    """
    churn_dir = tempfile.mkdtemp(dir=workspace_dir)
    LOGGER.debug('processing percentiles for %s', raster_path)
    heap_size = 2**28
    ffi_buffer_size = 2**10
    percentile_values_list = pygeoprocessing.raster_band_percentile(
        (raster_path, 1), churn_dir, percentiles_list,
        heap_size, ffi_buffer_size)
    with open(result_pickle_path, 'wb') as pickle_file:
        pickle.dump(percentile_values_list, pickle_file)
        pickle_file.flush()
    shutil.rmtree(churn_dir)


def mask_op(base_array, mask_array):
    """Mask base array assume nodata is -1."""
    base_array[mask_array == 1] = -1
    return base_array


if __name__ == '__main__':
    FH = logging.FileHandler('normalize_log.txt')
    FH.setLevel(logging.DEBUG)
    LOGGER.addHandler(FH)

    TASK_GRAPH = taskgraph.TaskGraph('.', int(sys.argv[1]))

    WORKSPACE_DIR = 'normalize_workspace'
    ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshard')
    CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')
    for dir_path in [WORKSPACE_DIR, ECOSHARD_DIR, CHURN_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    MASK_RASTER_URL = 'https://storage.googleapis.com/critical-natural-capital-ecoshards/masked_nathab_esa_nodata_md5_7c9acfe052cb7bdad319f011e9389fb1.tif'
    MASK_RASTER_PATH = os.path.join(ECOSHARD_DIR, os.path.basename(MASK_RASTER_URL))
    DOWNLOAD_MASK_TASK = TASK_GRAPH.add_task(
        func=ecoshard.download_url,
        args=(MASK_RASTER_URL, MASK_RASTER_PATH),
        target_path_list=[MASK_RASTER_PATH],
        task_name='download mask')

    RASTERS_TO_MASK_AND_NORMALIZE_URL_LIST = [
        'https://storage.googleapis.com/critical-natural-capital-ecoshards/realized_nwfp_masked_md5_a907048c3cc62ec51640048bb710d8d8.tif',
        'https://storage.googleapis.com/critical-natural-capital-ecoshards/potential_grazing_md5_cf6c597be3b0df9b8379c16c732d3ee7.tif',
        'https://storage.googleapis.com/critical-natural-capital-ecoshards/potential_nitrogenretention_md5_286c51393042973f71884ddc701be03d.tif',
        'https://storage.googleapis.com/critical-natural-capital-ecoshards/potential_pollination_all_md5_b91afbddd0576c7951ec08864a1b08ef.tif',
        'https://storage.googleapis.com/critical-natural-capital-ecoshards/potential_sedimentdeposition_md5_aa9ee6050c423b6da37f8c2723d9b513.tif',
        'https://storage.googleapis.com/critical-natural-capital-ecoshards/potential_wood_products_md5_a5429e1381d35e6632a16d550147ff32.tif',
        'https://storage.googleapis.com/critical-natural-capital-ecoshards/realized_grazing_md5_19085729ae358e0e8566676c5c7aae72.tif',
        'https://storage.googleapis.com/critical-natural-capital-ecoshards/realized_nitrogenretention_downstream_md5_82d4e57042482eb1b92d03c0d387f501.tif',
        'https://storage.googleapis.com/critical-natural-capital-ecoshards/realized_pollination_md5_443522f6688011fd561297e9a556629b.tif',
        'https://storage.googleapis.com/critical-natural-capital-ecoshards/realized_sedimentdeposition_downstream_md5_1613b12643898c1475c5ec3180836770.tif',
        'https://storage.googleapis.com/critical-natural-capital-ecoshards/realized_timber_md5_340467b17d0950d381f55cd355ae688a.tif']

    RASTERS_TO_NORMALIZE_PATH_LIST = []
    LOCAL_RASTER_PATH_LIST = []
    for URL in RASTERS_TO_MASK_AND_NORMALIZE_URL_LIST:
        LOCAL_RASTER_PATH = os.path.join(ECOSHARD_DIR, os.path.basename(URL))
        LOGGER.debug('downloading %s', LOCAL_RASTER_PATH)
        DOWNLOAD_TASK = TASK_GRAPH.add_task(
            func=ecoshard.download_url,
            args=(URL, LOCAL_RASTER_PATH),
            target_path_list=[LOCAL_RASTER_PATH],
            task_name='download %s' % LOCAL_RASTER_PATH)
        LOCAL_RASTER_PATH_LIST.append(LOCAL_RASTER_PATH)

    TASK_GRAPH.join()

    for LOCAL_RASTER_PATH in LOCAL_RASTER_PATH_LIST:
        MASKED_LOCAL_RASTER_PATH = os.path.join(
            CHURN_DIR, 'masked_%s' % os.path.basename(LOCAL_RASTER_PATH))
        LOGGER.debug('masking %s', MASKED_LOCAL_RASTER_PATH)
        REMASKING_EXPRESSION = {
            'expression': 'mask*service',
            'symbol_to_path_map': {
                'mask': MASK_RASTER_PATH,
                'service': LOCAL_RASTER_PATH,
            },
            'target_nodata': -1,
            'target_raster_path': MASKED_LOCAL_RASTER_PATH,
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        }

        raster_calculations_core.evaluate_calculation(
            REMASKING_EXPRESSION, TASK_GRAPH, WORKSPACE_DIR)
        RASTERS_TO_NORMALIZE_PATH_LIST.append(
            MASKED_LOCAL_RASTER_PATH)

    NORMALIZE_THESE_DIRECTLY = [
        'https://storage.googleapis.com/critical-natural-capital-ecoshards/normalized_potential_moisture_md5_d5396383d8a30f296988f86bb0fc0528.tif',
        'https://storage.googleapis.com/critical-natural-capital-ecoshards/normalized_potential_flood_md5_6b603609e55d3a17d20ea76699aaaf79.tif',
        'https://storage.googleapis.com/critical-natural-capital-ecoshards/normalized_realized_flood_md5_f1237e76a41039e22629abb85963ba16.tif']

    for URL in RASTERS_TO_MASK_AND_NORMALIZE_URL_LIST:
        LOCAL_RASTER_PATH = os.path.join(ECOSHARD_DIR, os.path.basename(URL))
        DOWNLOAD_TASK = TASK_GRAPH.add_task(
            func=ecoshard.download_url,
            args=(URL, LOCAL_RASTER_PATH),
            target_path_list=[LOCAL_RASTER_PATH],
            task_name='download %s' % LOCAL_RASTER_PATH)
        RASTERS_TO_NORMALIZE_PATH_LIST.append(LOCAL_RASTER_PATH)

    WORLD_BORDERS_URL = 'https://storage.googleapis.com/ecoshard-root/critical_natural_capital/TM_WORLD_BORDERS-0.3_simplified_md5_47f2059be8d4016072aa6abe77762021.gpkg'
    WORLD_BORDERS_PATH = os.path.join(
        ECOSHARD_DIR, os.path.basename(WORLD_BORDERS_URL))
    _ = TASK_GRAPH.add_task(
        func=ecoshard.download_url,
        args=(WORLD_BORDERS_URL, WORLD_BORDERS_PATH),
        target_path_list=[WORLD_BORDERS_PATH],
        task_name='download %s' % WORLD_BORDERS_PATH)

    LOGGER.debug("waiting for everything to download")
    TASK_GRAPH.join()
    LOGGER.debug("starting to normalize everything")
    for PATH in RASTERS_TO_NORMALIZE_PATH_LIST:
        BASE_NAME = os.path.splitext(os.path.basename(PATH))[0]
        NORMALIZE_WORKSPACE_DIR = os.path.join(WORKSPACE_DIR, BASE_NAME)
        TARGET_PATH = os.path.join(
            WORKSPACE_DIR, 'normalized_%s.tif' % BASE_NAME)
        normalize_by_polygon(
            PATH, WORLD_BORDERS_PATH, 99, [0, 1], NORMALIZE_WORKSPACE_DIR,
            TARGET_PATH)

    TASK_GRAPH.join()
    TASK_GRAPH.close()
