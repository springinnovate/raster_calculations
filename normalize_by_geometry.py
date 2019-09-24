"""Normalize a raster by percentile in a polygon.

    * given a polygon make a local stats list there.
    * option to clamp 0..1

"""
import sys
import pickle
import shutil
import os
import tempfile
import logging

import numpy
import shapely.wkb
from osgeo import gdal
from osgeo import ogr
import taskgraph
import pygeoprocessing

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


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
        _ = TASK_GRAPH.add_task(
            func=calculate_percentile,
            args=(feature_mask_path, [percentile], base_dir,
                  percentile_pickle_path),
            target_path_list=[percentile_pickle_path],
            dependent_task_list=[mask_raster_task],
            task_name='calculating %s' % percentile_pickle_path)
        fid_to_percentile_pickle_path[fid] = percentile_pickle_path
        feature = None

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
         (raster_nodata, 'raw'), (-1, 'raw'), (-1, 'raw')], divide_op,
        target_path, gdal.GDT_Float32, -1)


def divide_op(raster_a, raster_b, a_nodata, b_nodata, target_nodata):
    """Divide a by b."""
    result = numpy.empty(raster_a.shape)
    result[:] = target_nodata
    valid_mask = (
        (~numpy.isclose(raster_a, a_nodata)) &
        (~numpy.isclose(raster_b, b_nodata)) &
        (raster_b != 0.0))
    result[valid_mask] = raster_a[valid_mask] / raster_b[valid_mask]
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


if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph('.', 4)
    RASTER_PATH = 'local_data/potential_pollination_edge_md5_3b0171d8dac47d2aa2c6f41fb94b6243.tif'
    VECTOR_PATH = 'local_data/TM_WORLD_BORDERS_SIMPL-0.3_md5_c0d1b65f6986609031e4d26c6c257f07.gpkg'
    TARGET_PATH = 'normalize_workspace/normalized_by_country.tif'
    normalize_by_polygon(
        RASTER_PATH, VECTOR_PATH, 97, [0, 1], 'normalize_workspace/churn',
        TARGET_PATH)
