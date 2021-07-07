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

MASK_ECOSHARD_URL = (
    #'https://storage.googleapis.com/critical-natural-capital-ecoshards/nature_access/' #this is not a great one
    #'global_people_access_lspop_2017_URCA_rural_360.0m_md5_14991c229634505edd5ff3f1944acda1.tif')
    #'https://storage.googleapis.com/critical-natural-capital-ecoshards/masked_all_nathab_wstreams_esa2015_nodata_WARPED_near_md5_d801fffb0e3fbfd8d7ffb508f18ebb7c.tif')
    #'https://storage.googleapis.com/critical-natural-capital-ecoshards/habmasks/landmask_10s_md5_748981cbf6ebf22643a3a3e655ec50ce_compressed.tif')
    'https://storage.googleapis.com/critical-natural-capital-ecoshards/habmasks/landmask_10s_md5_748981cbf6ebf22643a3a3e655ec50ce_compressed_reduce8x.tif')

#ECOSHARD_URL_PREFIX = 'https://storage.googleapis.com/critical-natural-capital-ecoshards/realized_service_ecoshards/truncated_masked'
ECOSHARD_URL_PREFIX = 'https://storage.googleapis.com/critical-natural-capital-ecoshards/optimization_results'


# Format of these are (ecoshard filename, mask(t/f), perarea(t/f))
RASTER_LIST = [
#    ('realized_coastalprotection_barrierreef_md5_126320d42827adc0f7504d4693c67e18.tif', False, False),
#    ('realized_coastalprotection_md5_.tif', True, True),
#    ('realized_coastalprotection_offshore_md5_.tif', False, True),
#    ('realized_commercialtimber_forest_clamped0_md5_24844213f0f65a6c0bedfebe2fbd089e.tif', True, False),
#    ('realized_domestictimber_forest_clamped0_md5_dca99ceb7dd9f96d54b3fcec656d3180.tif', True, False),
#    ('realized_flood_nathab_clamped0_md5_eb8fd58621e00c6aeb80f4483da1b35c.tif', True, False),
#    ('realized_fuelwood_forest_clamped0_md5_4ee236f5400ac400c07642356dd358d1.tif', True, False),
#    ('realized_fwfish_per_km2_clamped_1e-3_30_md5_0b4455185988a9e2062a39b27910eb8b.tif', True, False),
#    ('realized_grazing_natnotforest_clamped0_md5_8eeb02139f0fabf552658f7641ab7576.tif', True, False),
#    ('realized_marinefish_watson_2010_2014_clamped_md5_167448a2c010fb2f20f9727b024efab8.tif', False, False),
#    ('realized_nitrogenretention_nathab_clamped_md5_.tif', True, True),
#    ('realized_pollination_nathab_clamped_md5_.tif', True, True),
#    ('realized_reeftourism_Modelled_Total_Dollar_Value_md5_171a993b8ff40d0447f343dd014c72e0.tif', False, False),
#    ('realized_sedimentdeposition_nathab_clamped_md5_.tif', True, True),
#    ('Vulnerable_C_Total_2018_md5_9ab63337d8b4a6c6fd4f7f597a66ffed.tif', True, False),
#    ('realized_moisturerecycling_nathab30s_md5_6c97073919f952545349efcc95d4ea7f.tif', True, False),
#    ('realized_coastalprotection_norm_md5_485aef1d6c412bde472bdaa1393100d7.tif', False, False),
#    ('realized_coastalprotection_norm_offshore_md5_68a2b938e3712f3cf764fb0be8f54685.tif', False, False),
#    ('realized_floodmitigation_norm_md5_ba11bb43399648b9b010493707885418.tif', True, False),
#    ('realized_nitrogenretention_norm_md5_504bf57e964592f2494b0a1fb8ef0dcd.tif', True, False),
#    ('realized_pollination_norm_nathab_clamped_md5_2a832d086de6a857d26cb30915b948b1.tif', True, False),
#    ('realized_sedimentdeposition_norm_md5_6fa5ff49ba93e01927efd37f7b91bfd7.tif', True, False),]
#    ('realized_nitrogenretention_attn_500km_md5_fe9a89318d97a48dfd78dbeed4cb2b94.tif', True, False),
#    ('realized_sedimentdeposition_attn_500km_md5_133ecc403c8c572f93157b659ea79d02.tif', True, False),
#    ('realized_nitrogenretention_attn_50km_md5_5d5804f5bfed6bd1025a51128c7ec835.tif', True, False),
#    ('realized_sedimentdeposition_attn_50km_md5_5139215a28962a1c1cd2d18ca40b7244.tif', True, False),
#    ('realized_floodmitigation_attn_500km_nathab_clamped_md5_db7fb0cebfc9dc0979b93fe36eca62ee.tif', True, False),
#    ('realized_floodmitigation_attn_50km_nathab_clamped_md5_c7f0f1ee91571cdf94bbb43bd50eb563.tif', True, False),
#    ('A90_md5_a227d61b8e1469fbd3abd34d045435e4.tif', True, False),
    ('A90_md5_a227d61b8e1469fbd3abd34d045435e4.tif', True, False),
    ('D1_90_md5_9633e6b25a1c28632588e6bbef7e0715.tif', True, False),
    ('D_90_md5_cd7a02778615934d66e5be1ad5df11a8.tif', True, False),
    ('E1_90_md5_6cfb84c71a3acc0019181b83d94236ea.tif', True, False),
    ('E_90_md5_58da94a0d6b17245b485c9c8bf8fc071.tif', True, False),
    ('F1_90_md5_1154967b32728ff635a4239470575a0d.tif', True, False),
    ('F_90_md5_d567ef8f70fd4bb76852aaa50c2bf4ff.tif', True, False),
    ('G1_90_md5_021588f5ee9356448ebf39010af355c4.tif', True, False),
    ('G_90_md5_52557cebef77f8c6db3f15ba91343626.tif', True, False),
    ('H1_90_md5_d79e07071bd6820241799ee9a033065e.tif', True, False),
    ('H_90_md5_fe35d7c6984622a1429ff0037b5cab69.tif', True, False),
    ('I1_90_md5_6b7969887db7a3cdf4589a86f92bfdbf.tif', True, False),
    ('I_90_md5_b6bf46b9e8b1ed00af0b790dbb3e5588.tif', True, False),
    ('J_90_md5_39938228076e62c4efed0759089cb0a6.tif', True, False),
    ('K_90_md5_804d07eeb3a0400fa4fa59672c1dd8ec.tif', True, False),
    ('L_90_md5_528091cef25d701d215f6b4838adfcf4.tif', True, False),
    ('M_90_md5_3f324797bd8a606341bc91c43a582a54.tif', True, False),
    ('N_90_md5_4c7f2897f51c2937033c5edbe45a98f2.tif', True, False),
    ('O_90_md5_cf6bbb7b46a1901419cb1ad7bc444760.tif', True, False),
    ('P_90_md5_f976d517118f0ed549e65d71a5d2fd58.tif', True, False),
    ('Q_90_md5_4e9dc8c0820552327df3740070363cf0.tif', True, False),
    ('R_90_md5_83f855316180ee8f387f79d3fee19fca.tif', True, False),
    ('S_90_md5_569e79f8bf753457b3fdcba325c8aab9.tif', True, False),
    ('T_90_md5_f0f6ca5add6d3a98561e81fdaefe7237.tif', True, False),
    ('U_90_md5_14568cbf3cc6401d0a894f8daeed144d.tif', True, False),
    ('V_90_md5_eade0f1e226c6fe0bbf7f1ca64bfca99.tif', True, False),
    ('W_90_md5_e8e99efe1cb5f61f9fa00630649b7eea.tif', True, False),
    ('X_90_md5_891eac3d67e5efc66909131640d2794a.tif', True, False),
    ('Y_90_md5_68117f49cd41e41f3a8915a2a8c941b1.tif', True, False),
    ('Z_90_md5_60e9c2e61674058dd327af159d19aaf9.tif', True, False),
    ('solution_A_alltargets_2km_md5_743cf8932c76bcb2480200aaff44d137.tif', True, False),
    ('solution_B_alltargets_2km_md5_c7468d1b14d3086915f32f49265e9dda.tif', True, False),
    ('solution_C_alltargets_2km_md5_ecc7ac5e9ae240495497b641cd65496d.tif', True, False),
    ]


WARPED_SUFFIX = '_WARPED'
MASKED_SUFFIX = '_MASKED'
PERAREA_SUFFIX = '_PERAREA'
RESAMPLE_MODE = 'average'

WORKSPACE_DIR = 'workspace'
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
        args=(MASK_ECOSHARD_URL, mask_ecoshard_path),
        target_path_list=[mask_ecoshard_path],
        task_name=f'download {mask_ecoshard_path}')

    for ecoshard_base, mask_flag, per_area_flag in RASTER_LIST:
        ecoshard_url = os.path.join(ECOSHARD_URL_PREFIX, ecoshard_base)
        target_path = os.path.join(ECOSHARD_DIR, ecoshard_base)
        LOGGER.debug(f'download {ecoshard_url} to {target_path}')
        last_task = task_graph.add_task(
            func=ecoshard.download_url,
            args=(ecoshard_url, target_path),
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
