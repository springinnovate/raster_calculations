"""Align given rasters to given bounding box and projection."""
import logging
import multiprocessing
import os
import re
import shutil
import sys

from osgeo import gdal
from osgeo import osr
from ecoshard.geoprocessing import _create_latitude_m2_area_column
import ecoshard
import numpy
from ecoshard import geoprocessing
from ecoshard import taskgraph

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
#    'https://storage.googleapis.com/ecoshard-root/population/global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif')
#    "file:///D:/repositories/raster_calculations/align_to_mask_workspace/ecoshards/global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif")
#    'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/habitat_masks/Ecoregions2017_ESA2020modVCFv2_zones_Argentina_compressed_md5_806575.tif')
#    'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/habitat_masks/Ecoregions2017_ESA2020modVCFv2_zones_Indonesia_compressed_md5_226d32.tif')
#    'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/habitat_masks/Ecoregions2017_ESA2020modVCFv2_zones_US_compressed_md5_b62ebe.tif')
#2#    'https://storage.googleapis.com/critical-natural-capital-ecoshards/habmasks/iucn_ecosystems/T2_4_Warm_temp_rainforests.tif')
#    'https://storage.googleapis.com/sci-ncscobenefits-spring/data/AFC_Tree2050_compressed_md5_3e88e1.tif')
#unilever#    "file:///D:/Documents/unilever_archive/carbon_edge_model/carbon_model_workspace/data/baccini_carbon_data_2003_2014_compressed_md5_11d1455ee8f091bf4be12c4f7ff9451b.tif")
#    "file:///D:/repositories/tnc-sci-ncscobenefits/ecoshards/AFC_Tree2050_compressed_md5_3e88e1.tif")
#unilever2#    "file:///D:/repositories/carbon_edge_model/output/regression_carbon_esa.tif")
#tnc#    "file:///D:/repositories/tnc-sci-ncscobenefits/ecoshards/area_suit_SilvoArable_frac_md5_15b589.tif")
#    "file:///D:/repositories/pollination_sufficiency/workspace_poll_suff/outputs_tnc_esa2020/pollination_ppl_fed_on_ag_10s_tnc_esa2020.tif")
#    "file:///D:/ecoshard/TNC_NBS/results/cv_habitat_value_marESA2020_md5_bc7bb3.tif")
#    "file:///D:/repositories/ndr_sdr_global/workspace/data/marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da.tif")
#sd#    "file:///D:/repositories/raster_calculations/reclassed_iucn_ecotypes_md5_b0f3be.tif")
#c    "file:///D:/repositories/carbon_edge_model/output_global/regression_optimization/regressioncoarsened_marginal_value_regression_mask_3500000280000.0.tif")
#c    "file:///D:/repositories/raster_calculations/align_to_mask_workspace/ecoshards/ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_2ed6285e6f8ec1e7e0b75309cc6d6f9f.tif")
#tnc0414_1#    "file:///D:/repositories/tnc-sci-ncscobenefits/scenarios/DIFF_reforest2-ESA.tif")
#tnc0414_2#    "file:///D:/repositories/tnc-sci-ncscobenefits/scenarios/DIFF_agroforest-ESA.tif")
#    "file:///D:/repositories/wwf-sipa/data/pop/phl_ppp_2020.tif")
    "file:///D:/repositories/wwf-sipa/data/pop/idn_ppp_2020.tif")

#ECOSHARD_URL_PREFIX = 'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/results/pollination'
#ECOSHARD_URL_PREFIX = 'https://storage.googleapis.com/ecoshard-root/sci-ncscobenefits-spring/data'
#unilever#ECOSHARD_URL_PREFIX = 'https://storage.googleapis.com/ecoshard-root/carbon_datasets/'
#2#ECOSHARD_URL_PREFIX = 'https://storage.googleapis.com/critical-natural-capital-ecoshards/optimization_results/single_service'
#ECOSHARD_URL_PREFIX = 'https://storage.googleapis.com/sci-ncscobenefits-spring/data'
#ECOSHARD_URL_PREFIX = "file:///D:/Documents/unilever_archive/carbon_rasters_2022/"
#ECOSHARD_URL_PREFIX = "file:///D:/repositories/tnc-sci-ncscobenefits/NCS_Refor11_map/"
#unilever2#ECOSHARD_URL_PREFIX = "file:///D:/repositories/carbon_edge_model/processed_rasters/"
#tnc#ECOSHARD_URL_PREFIX = "file:///D:/repositories/tnc-sci-ncscobenefits/ecoshards/"
#ECOSHARD_URL_PREFIX = "file:///D:/repositories/tnc-sci-ncscobenefits/scenarios/fertilizers/"
#ECOSHARD_URL_PREFIX = "file:///D:/repositories/pollination_sufficiency/workspace_poll_suff/outputs_tnc_nbs_mangroverest/"
#ECOSHARD_URL_PREFIX = "file:///D:/ecoshard/TNC_NBS/results/"
#ECOSHARD_URL_PREFIX = "file:///D:/repositories/ndr_sdr_global/workspace/data/"
#tnc2#ECOSHARD_URL_PREFIX = "file:///D:/repositories/tnc-sci-ncscobenefits"
#sd#ECOSHARD_URL_PREFIX = "file:///D:/repositories/raster_calculations"
#c#ECOSHARD_URL_PREFIX = "file:///D:/repositories/carbon_edge_model/supporting_data"
#tnc0414#ECOSHARD_URL_PREFIX = "file:///D:/repositories/tnc-sci-ncscobenefits/carbon_data"
#tnc3#ECOSHARD_URL_PREFIX = "file:///D:/repositories/raster_calculations/align_to_mask_workspace/ecoshards"
ECOSHARD_URL_PREFIX = "file:///Z:/bck_archive/cna_analyses_archive/original_rasters/downstream_beneficiaries/floodplains"

# Format of these are (ecoshard filename, mask(t/f), perarea(t/f), in wgs84 projection)
RASTER_LIST = [
    #('ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_2ed6285e6f8ec1e7e0b75309cc6d6f9f_hab_mask.tif', False, False),
    #('restoration_pnv0.0001_on_ESA2020_v2_md5_47613f8e4d340c92b2c481cc8080cc9d_hab_mask.tif', False, False),
    #('results/global_normalized_people_access_population_2019_60.0m_md5_6a3bf3ec196b3b295930e75d8808fa9c.tif', True, True, False),
    #('results/global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif', True, True, False),
    #('reclassified_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46_esa_to_nathab_forest_mask.tif', False, False, False),
    #('reclassified_Sc1v3_restoration_pnv0.5_on_ESA2020mVCF_md5_403f35b2a8b9b917090703e291f6bc0c_esa_to_nathab_forest_mask.tif', False, False, False),
    #('reclassified_Sc1v4_restoration_pnv0.001_on_ESA2020mVCF_md5_61a44df722532a84a77598fe2a24d46c_esa_to_nathab_forest_mask.tif', False, False, False),
    #('reclassified_Sc2v3_Griscom_CookPatton2050_smithpnv_md5_82c2f863d49f5a25c0b857865bfdb4b0_esa_to_nathab_forest_mask.tif', False, False, False),
    #('reclassified_Sc2v4_Griscom_CookPatton2035_smithpnv_md5_ffde2403583e30d7df4d16a0687d71fe_esa_to_nathab_forest_mask.tif', False, False, False),
    #('Sc3v1_PNVnoag_md5_c07865b995f9ab2236b8df0378f9206f_hab_mask.tif', False, False, False)
    #('marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-1992-v2.0.7cds_compressed_md5_83ec1b_hab_mask.tif', False, False, False),
    #('marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da_hab_mask.tif', False, False, False)
    #('CLUMONDO_livestock_ssp1_change_md5_8b9543.tif', False, False, False),
    #('CLUMONDO_livestock_ssp3_change_md5_578b3b.tif', False, False, False)
    #('Total_Carbon_2010_compressed_md5_62290b.tif', True, False, True)
    #2#('A1_90_md5_1fb33de8a6ced1d1f54dcc7debed3c6c.tif', False, False, False),
    #2#('D1_90_md5_ee81ad59355f2309c2ecb882e788454a.tif', False, False, False),
    #2#('B1_90_md5_14484122eba5a970559c57a48621d3fd.tif', False, False, False),
    #2#('U_90_md5_258160b638e742e91b84979e6b2c748f.tif', False, False, False),
    #2#('S_90_md5_5d18924c69519ec76993f4d58a7b2687.tif', False, False, False),
    #2#('H1_90_md5_7973783ac2786f9d521a4b8b4cf5d68d.tif', False, False, False),
    #2#('I1_90_md5_54ad2f227abc1cf66ed23cc6d3b72d47.tif', False, False, False)
    #('AFC_threats_Curtis_orig_proj_md5_f2c653.tif', False, False, False)
    #unilever#('fc_stack_hansen_forest_cover2003_carbon_reduce8x.tif', False, False, False),
    #('fc_stack_hansen_forest_cover2004_carbon_reduce8x.tif', False, False, False),
    #('fc_stack_hansen_forest_cover2006_carbon_reduce8x.tif', False, False, False),
    #('fc_stack_hansen_forest_cover2007_carbon_reduce8x.tif', False, False, False),
    #('fc_stack_hansen_forest_cover2008_carbon_reduce8x.tif', False, False, False),
    #('fc_stack_hansen_forest_cover2009_carbon_reduce8x.tif', False, False, False),
    #('fc_stack_hansen_forest_cover2010_carbon_reduce8x.tif', False, False, False),
    #('fc_stack_hansen_forest_cover2011_carbon_reduce8x.tif', False, False, False),
    #('fc_stack_hansen_forest_cover2012_carbon_reduce8x.tif', False, False, False),
    #tnc#('reclassified_NCS_Refor11_map_NCS_Refor_reclass.tif', False, False, False), #rename output Griscom_extent_wgs
    #unilever2#('baccini_carbon_data_2014_compressed.tif', False, False, False)
    #tnc#('area_suit_SilvoPasture_Tot_frac_md5_6f300c.tif', False, False, False)
    #('ExtensificationNapp_allcrops_rainfedfootprint_gapfilled_observedNappRevB_capped_to_backgroundN_md5_3de812.tif', False, False, False),
    #('extensification_current_practices_n_app_compressed_md5_3005a1.tif', False, False, False)
    #('pollination_ppl_fed_on_ag_10s_tnc_nbs_mangroverest.tif', False, False, False)
    #('cv_habitat_value_tnc_mangroverest.tif', False, False, False)
    #('mangroves_restore_2050_md5_eaa31b.tif', False, False, False)
    #tnc2#('reclassified_agroforestry_2050_md5_e32698_esa_nathab_reclass.tif', False, False, False),
    #tnc2#('reclassified_forest_conversion_2050_md5_abda51_esa_nathab_reclass.tif', False, False, False),
    #tnc2#('reclassified_mangroves_restore_2050_WARPED_near_md5_c88042_esa_nathab_reclass.tif', False, False, False),
    #tnc2#('reclassified_marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da_esa_nathab_reclass.tif', False, False, False),
    #tnc2#('reclassified_reforestation_full_griscom_extent_compressed_md5_e42c6c_esa_nathab_reclass.tif', False, False, False),
    #sd#('A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif', False, False, False),
    #sd#('C90_nonoverlapping_A90_md5_226cd195e0cb83760f866fb1f474ecb8.tif', False, False, False)
    #c#('LPD.tif', False, False, False),
    #c#('degraded_lands_on_regression_350mha.tif', False, False, False)
    #tnc0414_1#('carbon_seq_30yrs_Mg_C_ha.tif', True, False, False)
    #tnc0414_2#('carbon_seq_30yrs_10pct_Mg_C_ha.tif', True, False, False)
    #tnc0414#('hab_mask_tnc_nbs_afc2.tif', False, False, False),
    #tnc0414#('hab_mask_tnc_nbs_reforest2.tif', False, False, False)
    #tnc3#('DIFF_agroforest-ESA_md5_3d054f.tif', False, False, False),
    #tnc3#('DIFF_mangroverest-ESA_md5_a53b7a.tif', False, False, False),
    #tnc3#('DIFF_reforest2-ESA_md5_d53d15.tif', False, False, False),
    #tnc3#('DIFF_afc2-ESA_md5_ce1730.tif', False, False, False)
    ('global_floodplains_mask.tif', True, False, True)
    ]

WARPED_SUFFIX = '_WARPED'
MASKED_SUFFIX = '_MASKED'
PERAREA_SUFFIX = '_PERAREA'
RESCALED_VALUE_SUFFIX = '_AREA_SCALED_VALUE'
RESAMPLE_MODE = 'average'
#tnc#RESAMPLE_MODE = 'near'
#unilever#RESAMPLE_MODE = 'average'

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
    base_projection_wkt = geoprocessing.get_raster_info(
        base_raster_path)['projection_wkt']
    if base_projection_wkt is None:
        # assume its wgs84 if not defined
        LOGGER.warn(
            f'{base_raster_path} has undefined projection, assuming WGS84')
        base_projection_wkt = osr.SRS_WKT_WGS84_LAT_LONG
    mask_raster_info = geoprocessing.get_raster_info(mask_raster_path)
    geoprocessing.warp_raster(
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
    try:
        ecoshard.hash_file(target_md5_free_path, rename=True)
    except OSError:
        LOGGER.exception(
            'hash file failed, possibly because file exists but that is okay '
            'since it is the same hash')


def mask_raster(base_raster_path, mask_raster_path, target_raster_path):
    """Mask base by mask setting nodata to nodata otherwise passthrough."""
    mask_nodata = geoprocessing.get_raster_info(
        mask_raster_path)['nodata'][0]
    base_raster_info = geoprocessing.get_raster_info(base_raster_path)
    base_nodata = base_raster_info['nodata'][0]

    def _mask_op(base_array, mask_array):
        result = numpy.copy(base_array)
        nodata_mask = numpy.isclose(mask_array, mask_nodata)
        result[nodata_mask] = base_nodata
        return result

    geoprocessing.raster_calculator(
        [(base_raster_path, 1), (mask_raster_path, 1)], _mask_op,
        target_raster_path, base_raster_info['datatype'], base_nodata)


def _convert_to_density(
        base_raster_path, wgs84_flag, target_density_raster_path):
    base_raster_info = geoprocessing.get_raster_info(
        base_raster_path)
    if wgs84_flag:
        ## xmin, ymin, xmax, ymax
        _, lat_min, _, lat_max = base_raster_info['bounding_box']
        _, n_rows = base_raster_info['raster_size']
        m2_area_col = _create_latitude_m2_area_column(lat_min, lat_max, n_rows)
    else:
        pixel_area = base_raster_info['pixel_size'][0]**2

    nodata = base_raster_info['nodata'][0]

    def _div_by_area_op(base_array, m2_area_array):
        result = numpy.empty(base_array.shape, dtype=base_array.dtype)
        if nodata is not None:
            valid_mask = ~numpy.isclose(base_array, nodata)
            result[:] = nodata
        else:
            valid_mask = numpy.ones(base_array.shape, dtype=bool)

        if wgs84_flag:
            result[valid_mask] = (
                base_array[valid_mask] / m2_area_array[valid_mask])
        else:
            result[valid_mask] = (
                base_array[valid_mask] / m2_area_array)
        return result

    if wgs84_flag:
        geoprocessing.raster_calculator(
            [(base_raster_path, 1), m2_area_col], _div_by_area_op,
            target_density_raster_path, base_raster_info['datatype'],
            nodata)
    else:
        geoprocessing.raster_calculator(
            [(base_raster_path, 1), (pixel_area, 'raw')], _div_by_area_op,
            target_density_raster_path, base_raster_info['datatype'],
            nodata)


def _wgs84_density_to_value(base_raster_path, target_value_raster_path):
    base_raster_info = geoprocessing.get_raster_info(
        base_raster_path)

    # xmin, ymin, xmax, ymax
    _, lat_min, _, lat_max = base_raster_info['bounding_box']
    _, n_rows = base_raster_info['raster_size']
    m2_area_col = _create_latitude_m2_area_column(lat_min, lat_max, n_rows)
    nodata = base_raster_info['nodata'][0]

    def _mult_by_area_op(base_array, m2_area_array):
        result = numpy.empty(base_array.shape, dtype=base_array.dtype)
        if nodata is not None:
            valid_mask = ~numpy.isclose(base_array, nodata)
            result[:] = nodata
        else:
            valid_mask = numpy.ones(base_array.shape, dtype=bool)

        result[valid_mask] = (
            base_array[valid_mask] * m2_area_array[valid_mask])
        return result

    geoprocessing.raster_calculator(
        [(base_raster_path, 1), m2_area_col], _mult_by_area_op,
        target_value_raster_path, base_raster_info['datatype'],
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

    #for ecoshard_base, mask_flag, per_area_flag in RASTER_LIST:
    for ecoshard_base, mask_flag, per_area_flag, wgs84_flag in RASTER_LIST:
        ecoshard_url = f'{ECOSHARD_URL_PREFIX}/{ecoshard_base}'
        target_path = os.path.join(ECOSHARD_DIR, ecoshard_base)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
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
                args=(target_path, wgs84_flag, wgs84_density_raster_path),
                target_path_list=[wgs84_density_raster_path],
                task_name=f'convert to density: {wgs84_density_raster_path}',
                dependent_task_list=[last_task],
                )
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

        if per_area_flag:
            # convert the density to a count
            wgs84_value_raster_path = os.path.join(
                PERAREA_DIR, f'%s{RESCALED_VALUE_SUFFIX}%s' % os.path.splitext(
                    os.path.basename(target_path)))
            last_task = task_graph.add_task(
                func=_wgs84_density_to_value,
                args=(target_path, wgs84_value_raster_path),
                target_path_list=[wgs84_value_raster_path],
                task_name=f'convert from density to value: {wgs84_value_raster_path}',
                dependent_task_list=[last_task],
                )
            task_graph.add_task(
                func=copy_and_rehash_final_file,
                args=(wgs84_value_raster_path, WORKSPACE_DIR),
                task_name=f'copy and reshash final target_path',
                dependent_task_list=[last_task])

    task_graph.close()
    task_graph.join()


if __name__ == '__main__':
    main()
