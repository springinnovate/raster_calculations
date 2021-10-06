"""Align given rasters to given bounding box and projection."""
import logging
import multiprocessing
import os
import re
import shutil
import sys

from osgeo import gdal
from osgeo import osr
from pygeoprocessing.geoprocessing import _create_latitude_m2_area_column
import ecoshard
import numpy
import pygeoprocessing
import taskgraph

gdal.SetCacheMax(2**27)

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.WARN)

MASK_ECOSHARD_URL = ( #NOTE THIS IS JUST FOR DATA/NODATA MASKS, 1/0 DOESN'T MATTER
    'https://storage.googleapis.com/ecoshard-root/cbd/esa2015_ssp3_realized_coastal_risk_reduction_per_ha_0.04205_md5_f0dfc4edfd033566f6a9815bd0529117.tif')

ECOSHARD_URL_PREFIX = 'https://storage.googleapis.com/ecoshard-root/cbd/'


# Format of these are (ecoshard filename, mask(t/f), perarea(t/f))
RASTER_LIST = [
    ('cv_value_esa2015_ssp3_md5_5c2daf7444cd7e942311c134e91cc7a2.tif', False, False),
    ('cv_value_pnv_ssp3_md5_4b30a987e4d0cd46895aee102e9d7df4.tif', False, False),
    ]


WARPED_SUFFIX = '_WARPED'
MASKED_SUFFIX = '_MASKED'
PERAREA_SUFFIX = '_PERAREA'
RESAMPLE_MODE = 'average'

WORKSPACE_DIR = 'align_to_mask_workspace'
PERAREA_DIR = os.path.join(WORKSPACE_DIR, 'per_area_rasters')
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshards')
MASK_DIR = os.path.join(WORKSPACE_DIR, 'mask')
WARPED_DIR = os.path.join(WORKSPACE_DIR, 'warped')

for dir_path in [
        WORKSPACE_DIR, PERAREA_DIR, ECOSHARD_DIR, MASK_DIR, WARPED_DIR]:
    os.makedirs(dir_path, exist_ok=True)


def warp_raster(base_raster_path, mask_raster_path, resample_mode, target_raster_path):
    """Warp raster to exemplar's bounding box, cell size, and projection."""
    base_projection_wkt = pygeoprocessing.get_raster_info(
        base_raster_path)['projection_wkt']
    if base_projection_wkt is None:
        # assume its wgs84 if not defined
        LOGGER.warn(
            f'{base_raster_path} has undefined projection, assuming WGS84')
        base_projection_wkt = osr.SRS_WKT_WGS84_LAT_LONG
    mask_raster_info = pygeoprocessing.get_raster_info(mask_raster_path)
    pygeoprocessing.warp_raster(
        base_raster_path, mask_raster_info['pixel_size'],
        target_raster_path, resample_mode,
        base_projection_wkt=base_projection_wkt,
        target_bb=mask_raster_info['bounding_box'],
        target_projection_wkt=mask_raster_info['projection_wkt'])


def copy_and_rehash_final_file(base_raster_path, target_dir):
    """Copy base to target and replace hash with current hash."""
    target_md5_free_path = os.path.join(
        target_dir,
        re.sub('(.*)md5_[0-9a-f]+_(.*)', r"\1\2", os.path.basename(
            base_raster_path)))
    shutil.copyfile(base_raster_path, target_md5_free_path)
    ecoshard.hash_file(target_md5_free_path, rename=True)


def mask_raster(base_raster_path, mask_raster_path, target_raster_path):
    """Mask base by mask setting nodata to nodata otherwise passthrough."""
    mask_nodata = pygeoprocessing.get_raster_info(
        mask_raster_path)['nodata'][0]
    base_raster_info = pygeoprocessing.get_raster_info(base_raster_path)
    base_nodata = base_raster_info['nodata'][0]

    def _mask_op(base_array, mask_array):
        result = numpy.copy(base_array)
        nodata_mask = numpy.isclose(mask_array, mask_nodata)
        result[nodata_mask] = base_nodata
        return result

    pygeoprocessing.raster_calculator(
        [(base_raster_path, 1), (mask_raster_path, 1)], _mask_op,
        target_raster_path, base_raster_info['datatype'], base_nodata)


def _convert_to_density(
        base_wgs84_raster_path, target_wgs84_density_raster_path):
    """Convert base WGS84 raster path to a per density raster path."""
    base_raster_info = pygeoprocessing.get_raster_info(
        base_wgs84_raster_path)
    # xmin, ymin, xmax, ymax
    _, lat_min, _, lat_max = base_raster_info['bounding_box']
    _, n_rows = base_raster_info['raster_size']

    m2_area_col = _create_latitude_m2_area_column(lat_min, lat_max, n_rows)
    nodata = base_raster_info['nodata'][0]

    def _div_by_area_op(base_array, m2_area_array):
        result = numpy.empty(base_array.shape, dtype=base_array.dtype)
        if nodata is not None:
            valid_mask = ~numpy.isclose(base_array, nodata)
            result[:] = nodata
        else:
            valid_mask = numpy.ones(base_array.shape, dtype=bool)

        result[valid_mask] = (
            base_array[valid_mask] / m2_area_array[valid_mask])
        return result

    pygeoprocessing.raster_calculator(
        [(base_wgs84_raster_path, 1), m2_area_col], _div_by_area_op,
        target_wgs84_density_raster_path, base_raster_info['datatype'],
        nodata)


def main():
    """Entry point."""
    task_graph = taskgraph.TaskGraph(
        WORKSPACE_DIR, multiprocessing.cpu_count(), 5.0)
    mask_ecoshard_path = os.path.join(
        ECOSHARD_DIR, os.path.basename(MASK_ECOSHARD_URL))
    download_mask_task = task_graph.add_task(
        func=ecoshard.download_url,
        args=(MASK_ECOSHARD_URL, mask_ecoshard_path, True), #True is for skip_target_if_exists parameter
        target_path_list=[mask_ecoshard_path],
        task_name=f'download {mask_ecoshard_path}')

    for ecoshard_base, mask_flag, per_area_flag in RASTER_LIST:
        ecoshard_url = f'{ECOSHARD_URL_PREFIX}/{ecoshard_base}'
        target_path = os.path.join(ECOSHARD_DIR, ecoshard_base)
        LOGGER.debug(f'download {ecoshard_url} to {target_path}')
        last_task = task_graph.add_task(
            func=ecoshard.download_url,
            args=(ecoshard_url, target_path, True),
            target_path_list=[target_path],
            dependent_task_list=[download_mask_task],
            task_name=f'download {ecoshard_url} to {target_path}')
        if per_area_flag:
            wgs84_density_raster_path = os.path.join(
                PERAREA_DIR, f'%s{PERAREA_SUFFIX}%s' % os.path.splitext(
                    os.path.basename(target_path)))
            last_task = task_graph.add_task(
                func=_convert_to_density,
                args=(target_path, wgs84_density_raster_path),
                target_path_list=[wgs84_density_raster_path],
                task_name=f'convert to density: {wgs84_density_raster_path}',
                dependent_task_list=[last_task])
            target_path = wgs84_density_raster_path
        warped_raster_path = os.path.join(
            WARPED_DIR,
            f'%s{WARPED_SUFFIX}_{RESAMPLE_MODE}%s' % os.path.splitext(
                os.path.basename(target_path)))

        last_task = task_graph.add_task(
            func=warp_raster,
            args=(
                target_path, mask_ecoshard_path, RESAMPLE_MODE,
                warped_raster_path),
            target_path_list=[warped_raster_path],
            task_name=f'warp raster {warped_raster_path}',
            dependent_task_list=[last_task])
        target_path = warped_raster_path
        if mask_flag:
            mask_raster_path = os.path.join(
                MASK_DIR,
                f'%s{MASKED_SUFFIX}%s' % os.path.splitext(os.path.basename(
                    warped_raster_path)))
            last_task = task_graph.add_task(
                func=mask_raster,
                args=(target_path, mask_ecoshard_path, mask_raster_path),
                target_path_list=[mask_raster_path],
                task_name=f'mask raster to {mask_raster_path}',
                dependent_task_list=[last_task])
            target_path = mask_raster_path

        task_graph.add_task(
            func=copy_and_rehash_final_file,
            args=(target_path, WORKSPACE_DIR),
            task_name=f'copy and reshash final target_path',
            dependent_task_list=[last_task])

    task_graph.close()
    task_graph.join()


if __name__ == '__main__':
    main()
