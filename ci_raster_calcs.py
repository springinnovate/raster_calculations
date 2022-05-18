"""These calculations are for the CI global restoration project."""
#conda activate py38_gdal312
#conda activate py39_ecoprocess

import glob
import sys
import os
import logging
import multiprocessing
import datetime
import subprocess
import raster_calculations_core
from osgeo import gdal
from osgeo import osr
#import taskgraph
#import pygeoprocessing
from ecoshard import taskgraph
import ecoshard.geoprocessing as pygeoprocessing

gdal.SetCacheMax(2**25)

WORKSPACE_DIR = 'CNC_workspace'
NCPUS = multiprocessing.cpu_count()
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


def main():
    """Write your expression here."""


    calculation_list = [
        {
            'expression': '(raster1>=190)*(raster1<210)*raster2 + (raster1<190)*raster1 + (raster1>=210)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\scenarios\Sc3v2_PNVallhab_md5_419ab9f579d10d9abb03635c5fdbc7ca.tif",
                'raster2': r"D:\archive\nci\PNV_smith_060420_md5_8dd464e0e23fefaaabe52e44aa296330.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\ecoshard\PNV_full_on_ESA.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    # gdal_translate -projwin 95.009507541 6.077102897 141.019509595 -11.007775829 -of GTiff -b1 2 D:/ecoshard/fc_stack/fc_stack_hansen_forest_cover_2000-2020_v2_compressed_md5_cd1f1f.tif D:/ecoshard/CI_FP/Indonesia/fc_2020_indonesia.tif
    # or can do this within QGIS clip raster by extend adding -b 21 to the additional command line parameters in advanced parameters

    calculation_list = [
        {
            'expression': '(raster1>0)*(raster1+raster2) + (raster1<1)*raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_FP\Indonesia\fc_2001_indonesia_compressed_md5_cc1953.tif",
                'raster2': r"D:\ecoshard\CI_FP\Indonesia\Viscose_BaselineExtent_compressed_md5_7bb6eb.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0002694945852,-0.0002694945852),
            'resample_method': 'near',
            'target_raster_path': r"D:\ecoshard\CI_FP\Indonesia\CurrentViscose_FC_Indonesia.tif",
        },
        {
            'expression': '(raster1>0)*(raster1+raster2) + (raster1<1)*raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_FP\Indonesia\fc_2001_indonesia_compressed_md5_cc1953.tif",
                'raster2': r"D:\ecoshard\CI_FP\Indonesia\Viscose_FutureExtent_compressed_md5_bb9ba8.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0002694945852,-0.0002694945852),
            'resample_method': 'near',
            'target_raster_path': r"D:\ecoshard\CI_FP\Indonesia\FutureViscose_FC_Indonesia.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    # python create_scenario.py "D:\ecoshard\CI_PPC\scenarios\ESAmodVCFv2_md5_05407ed305c24604eb5a38551cddb031.tif" "D:\ecoshard\CI_FP\Argentina\ESA2020_forest_lost_to_livestock_ssp1_md5_44b77f.tif" 0.9  --flip_target_val 130
    # python create_scenario.py "D:\ecoshard\CI_PPC\scenarios\ESAmodVCFv2_md5_05407ed305c24604eb5a38551cddb031.tif" "D:\ecoshard\CI_FP\Argentina\ESA2020_forest_lost_to_livestock_ssp3_md5_70d862.tif" 0.9  --flip_target_val 130

    calculation_list = [
        {
            'expression': '(raster1>64)*(raster2>0)', #very strange - on my machine QGIS shows values of 1, 2 and 4, but on bigboi it's 1, 64, and 256
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_FP\global_layers_to_clip\reclassified_ESAmodVCFv2_md5_05407ed305c24604eb5a38551cddb031_esa_to_nathab_forest_mask_v2.tif",
                'raster2': r"D:\ecoshard\CI_FP\global_layers_to_clip\CLUMONDO_livestock_ssp1_change_WARPED_near_md5_b7c0bdc292ffb93ac0571561a01d4f81.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': r"D:\ecoshard\CI_FP\global_layers_to_clip\ESA2020_forest_lost_to_livestock_ssp1.tif",
        },
        {
            'expression': '(raster1>64)*(raster2>0)',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_FP\global_layers_to_clip\reclassified_ESAmodVCFv2_md5_05407ed305c24604eb5a38551cddb031_esa_to_nathab_forest_mask_v2.tif",
                'raster2': r"D:\ecoshard\CI_FP\global_layers_to_clip\CLUMONDO_livestock_ssp3_change_WARPED_near_md5_d8eb9043514a23f122587e6a6693a016.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': r"D:\ecoshard\CI_FP\global_layers_to_clip\ESA2020_forest_lost_to_livestock_ssp3.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\leather\CLUMONDO_livestock_mask_2050ssp1.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\leather\CLUMONDO_livestock_mask_2000.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "CLUMONDO_livestock_ssp1_change.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\leather\CLUMONDO_livestock_mask_2050ssp3.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\leather\CLUMONDO_livestock_mask_2000.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "CLUMONDO_livestock_ssp3_change.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': '(raster1<13) + (raster1>13)*(raster1<15) + (raster1>17)*(raster1<22)',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\leather\CLUMONDO_ssp1_2050_EckertIV_md5_996864.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "CLUMONDO_livestock_mask_2050ssp1.tif",
        },
        {
            'expression': '(raster1<13) + (raster1>13)*(raster1<15) + (raster1>17)*(raster1<22)',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\leather\CLUMONDO_ssp3_2050_EckertIV_md5_56b1a1.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "CLUMONDO_livestock_mask_2050ssp3.tif",
        },
        {
            'expression': '(raster1<13) + (raster1>13)*(raster1<15) + (raster1>17)*(raster1<22)',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\leather\CLUMONDO_2000_EckertIV_md5_aad69b.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "CLUMONDO_livestock_mask_2000.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\ci-global-restoration\workspace\data\fc_2019_indonesia_md5_3f6187.tif",
                'raster2': r"D:\repositories\ci-global-restoration\workspace\data\fc_2019_indonesia_conv_nearest_to_edge_0.2Mha_md5_559903.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "fc_2019_conv0.2Mha_change.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\ci-global-restoration\workspace\global_sed_export_nlcd2016.tif",
                'raster2': r"D:\repositories\ci-global-restoration\workspace\global_sed_export_nlcd2016_cotton_to_83.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "sediment_NLCD_organic_cotton_change.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\ci-global-restoration\workspace\global_n_export_nlcd2016_fertilizer_current.tif",
                'raster2': r"D:\repositories\ci-global-restoration\workspace\global_n_export_nlcd2016_cotton_to_83_fertilizer_current.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "nitrogen_NLCD_organic_cotton_change.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\cv_habitat_value_Sc1v5-ESAmodVCFv2_md5_25d0c5.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc1v5-ESAmod2_v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\cv_habitat_value_Sc1v6_-ESAmodVCFv2_md5_75c77a.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc1v6_-ESAmod2_v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\cv_habitat_value_Sc2v5-ESAmodVCFv2_md5_768e57.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc2v5-ESAmod2_v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\cv_habitat_value_Sc2v6-ESAmodVCFv2_md5_217154.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc2v6-ESAmod2_v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\cv_habitat_value_Sc3v1-ESAmodVCFv2_md5_8cad1a.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc3v1-ESAmod2_v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\cv_habitat_value_Sc3v2-ESAmodVCFv2_md5_b696ea.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc3v2-ESAmod2_v2.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': 'raster1 * raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif",
                'raster2': r"D:\repositories\raster_calculations\align_to_mask_workspace\Sc3v1_PNVnoag_hab_mask_WARPED_near_md5_aec8d382951593fa531f7716ad56ddce.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "nature_access_lspop2019_Sc3v1_PNVnoag.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': 'raster1*-1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\cv_habitat_value_ESAmodVCFv2_md5_c01e9-Sc1v5_md5_d93c1.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc1v5-ESAmodVCFv2.tif",
        },
        {
            'expression': 'raster1*-1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\cv_habitat_value_ESAmodVCFv2_md5_c01e9-Sc2v5_md5_6b714f.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc2v5-ESAmodVCFv2.tif",
        },
        {
            'expression': 'raster1*-1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\ESAmodVCFv2_cv_habitat_value_md5_c01e9b17aee323ead79573d66fa4020d   -  Sc2v6_cv_habitat_value_md5_a14ded315c8e0ce2f2266a1c190e06ee.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc2v6-ESAmodVCFv2.tif",
        },
        {
            'expression': 'raster1*-1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\ESAmodVCFv2_cv_habitat_value_md5_c01e9b17aee323ead79573d66fa4020d   -  Sc3v1_cv_habitat_value_md5_e889c2dbc5783fc4c782fbd3b473d7de.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc3v1-ESAmodVCFv2.tif",
        },
        {
            'expression': 'raster1*-1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\ESAmodVCFv2_cv_habitat_value_md5_c01e9b17aee323ead79573d66fa4020d   -  Sc3v2_cv_habitat_value_md5_251c1c934b367c8181b873099d1118b8.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc3v2-ESAmodVCFv2.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    # Fixing the clip on Renato so it's the full extent
    #python stitch_rasters.py --target_projection_epsg 4326 --target_cell_size 0.00277777778 --target_raster_path  pollination_ppl_fed_on_ag_10s_Sc1Renato0_5.tif --resample_method near --overlap_algorithm replace --raster_list "D:\ecoshard\CI_PPC\ESAmodVCFv2_cv_habitat_value_md5_c01e9b17aee323ead79573d66fa4020d.tif" "D:\ecoshard\CI_PPC\pollination_ppl_fed_on_ag_10s_Sc1v3_clip_md5_8a1f7a28e75aec4859e7c0f07cc6282f.tif"

    # Have to use this for differencing because there are missing pixels in restoration (where ag got converted back)
    #python add_sub_missing_as_0.py --subtract "D:\ecoshard\CI_PPC\pollination_ppl_fed_on_ag_10s_Sc1Renato0_5_md5_3fe6b3.tif" "D:\ecoshard\CI_PPC\pollination_ppl_fed_on_ag_10s_esa2020_md5_0cf902.tif"
    #python add_sub_missing_as_0.py --subtract "D:\ecoshard\CI_PPC\pollination_ppl_fed_on_ag_10s_Sc2v3_Griscom2050_md5_a86e5f.tif" "D:\ecoshard\CI_PPC\pollination_ppl_fed_on_ag_10s_esa2020_md5_0cf902.tif"
    #python add_sub_missing_as_0.py --subtract "D:\ecoshard\CI_PPC\pollination_ppl_fed_on_ag_10s_Sc1v2_md5_28cb0.tif" "D:\ecoshard\CI_PPC\pollination_ppl_fed_on_ag_10s_esa2020_md5_0cf902.tif"
    #python add_sub_missing_as_0.py --subtract "D:\ecoshard\CI_PPC\pollination_ppl_fed_on_ag_10s_Sc2v4_Griscom2035_md5_ffee3.tif" "D:\ecoshard\CI_PPC\pollination_ppl_fed_on_ag_10s_esa2020_md5_0cf902.tif"

    calculation_list = [
        {
            'expression': '(raster1>=9999)*-9999 + (raster1<9999)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\pollination_sufficiency\pollination_ppl_fed_on_ag_10s_Sc1Renato0_5_md5_ccdae4   -  pollination_ppl_fed_on_ag_10s_esa2020_md5_0cf902.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "pollination_ppl_fed_on_ag_10s_Sc1Renato0_5_md5_ccdae4-esa2020_md5_0cf902_fixed.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': 'raster2 - raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\ci-global-restoration\workspace\global_n_export_nlcd2016_fertilizer_current.tif",
                'raster2': r"D:\repositories\ci-global-restoration\workspace\global_n_export_nlcd2016_cotton_to_83_fertilizer_current.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "nitrogen_NLCD_organic_cotton_change.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\cv_habitat_value_Sc1v5-ESAmodVCFv2_md5_25d0c5.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc1v5-ESAmod2_v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\cv_habitat_value_Sc1v6_-ESAmodVCFv2_md5_75c77a.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc1v6_-ESAmod2_v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\cv_habitat_value_Sc2v5-ESAmodVCFv2_md5_768e57.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc2v5-ESAmod2_v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\cv_habitat_value_Sc2v6-ESAmodVCFv2_md5_217154.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc2v6-ESAmod2_v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\cv_habitat_value_Sc3v1-ESAmodVCFv2_md5_8cad1a.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc3v1-ESAmod2_v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\cv_habitat_value_Sc3v2-ESAmodVCFv2_md5_b696ea.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc3v2-ESAmod2_v2.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': 'raster1 * raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif",
                'raster2': r"D:\repositories\raster_calculations\align_to_mask_workspace\Sc3v1_PNVnoag_hab_mask_WARPED_near_md5_aec8d382951593fa531f7716ad56ddce.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "nature_access_lspop2019_Sc3v1_PNVnoag.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return



    calculation_list = [
        {
            'expression': 'raster1*-1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\cv_habitat_value_ESAmodVCFv2_md5_c01e9-Sc1v5_md5_d93c1.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc1v5-ESAmodVCFv2.tif",
        },
        {
            'expression': 'raster1*-1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\cv_habitat_value_ESAmodVCFv2_md5_c01e9-Sc2v5_md5_6b714f.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc2v5-ESAmodVCFv2.tif",
        },
        {
            'expression': 'raster1*-1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\ESAmodVCFv2_cv_habitat_value_md5_c01e9b17aee323ead79573d66fa4020d   -  Sc2v6_cv_habitat_value_md5_a14ded315c8e0ce2f2266a1c190e06ee.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc2v6-ESAmodVCFv2.tif",
        },
        {
            'expression': 'raster1*-1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\ESAmodVCFv2_cv_habitat_value_md5_c01e9b17aee323ead79573d66fa4020d   -  Sc3v1_cv_habitat_value_md5_e889c2dbc5783fc4c782fbd3b473d7de.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc3v1-ESAmodVCFv2.tif",
        },
        {
            'expression': 'raster1*-1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CI_PPC\diff_maps\ESAmodVCFv2_cv_habitat_value_md5_c01e9b17aee323ead79573d66fa4020d   -  Sc3v2_cv_habitat_value_md5_251c1c934b367c8181b873099d1118b8.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_Sc3v2-ESAmodVCFv2.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    # Fixing the clip on Renato so it's the full extent
    #python stitch_rasters.py --target_projection_epsg 4326 --target_cell_size 0.00277777778 --target_raster_path  pollination_ppl_fed_on_ag_10s_Sc1Renato0_5.tif --resample_method near --overlap_algorithm replace --raster_list "D:\ecoshard\CI_PPC\pollination_ppl_fed_on_ag_10s_esa2020_md5_0cf902.tif" "D:\ecoshard\CI_PPC\pollination_ppl_fed_on_ag_10s_Sc1v3_clip_md5_8a1f7a28e75aec4859e7c0f07cc6282f.tif"
    #python stitch_rasters.py --target_projection_epsg 4326 --target_cell_size 0.00277777778 --target_raster_path  Sc1Renato0_5_hab_mask.tif --resample_method near --overlap_algorithm replace --raster_list "D:\repositories\pollination_sufficiency\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_2ed6285e6f8ec1e7e0b75309cc6d6f9f_hab_mask.tif" "D:\repositories\pollination_sufficiency\Sc1v3_clip_compressed_md5_182b5f085cbec0dc976135f00f810b7c_hab_mask.tif"

    # Have to use this for differencing because there are missing pixels in restoration (where ag got converted back)
    #NEED TO REDO THIS ONE!! #python add_sub_missing_as_0.py --subtract "D:\ecoshard\CI_PPC\pollination_ppl_fed_on_ag_10s_Sc1Renato0_5_md5_3fe6b3.tif" "D:\ecoshard\CI_PPC\pollination_ppl_fed_on_ag_10s_esa2020_md5_0cf902.tif"
    #python add_sub_missing_as_0.py --subtract "D:\ecoshard\CI_PPC\pollination_ppl_fed_on_ag_10s_Sc2v3_Griscom2050_md5_a86e5f.tif" "D:\ecoshard\CI_PPC\pollination_ppl_fed_on_ag_10s_esa2020_md5_0cf902.tif"
    #python add_sub_missing_as_0.py --subtract "D:\ecoshard\CI_PPC\pollination_ppl_fed_on_ag_10s_Sc1v2_md5_28cb0.tif" "D:\ecoshard\CI_PPC\pollination_ppl_fed_on_ag_10s_esa2020_md5_0cf902.tif"
    #python add_sub_missing_as_0.py --subtract "D:\ecoshard\CI_PPC\pollination_ppl_fed_on_ag_10s_Sc2v4_Griscom2035_md5_ffee3.tif" "D:\ecoshard\CI_PPC\pollination_ppl_fed_on_ag_10s_esa2020_md5_0cf902.tif"

    calculation_list = [
        {
            'expression': '(raster1>=9999)*-9999 + (raster1<9999)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\pollination_sufficiency\pollination_ppl_fed_on_ag_10s_Sc1Renato0_5_md5_ccdae4   -  pollination_ppl_fed_on_ag_10s_esa2020_md5_0cf902.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "pollination_ppl_fed_on_ag_10s_Sc1Renato0_5_md5_ccdae4-esa2020_md5_0cf902_fixed.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    calculation_list = [
        {
            'expression': 'raster2 - raster1',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\scenarios\scenario_masks\reclassified_ESAmodVCFv2_md5_05407ed305c24604eb5a38551cddb031_esa_to_nathab_forest_mask_v2.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenarios\scenario_masks\reclassified_Sc3v1_PNVnoag_md5_c07865b995f9ab2236b8df0378f9206f_esa_to_nathab_forest_mask_v2.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "Sc3v1-ESAmod2_changemask.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return



    calculation_list = [
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\difference maps\ESAmod2-Sc1v5_n_export_md5_f507bb53ac95f51e6a5a82867ddc5df3.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "ESAmod2-Sc1v5_n_export-v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\difference maps\ESAmod2-Sc1v5_sed_export_md5_791952edc0dd4c3383c6decd3950bdbb.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "ESAmod2-Sc1v5_sed_export-v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\difference maps\ESAmod2-Sc1v6_n_export_md5_ebd9ad0d5936c05f5bb451033ac59b38.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "ESAmod2-Sc1v6_n_export-v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\difference maps\ESAmod2-Sc1v6_sed_export_md5_b097fbc684ac35a707a34d41e1d8d800.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "ESAmod2-Sc1v6_sed_export-v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\difference maps\ESAmod2-Sc1v5_n_export_md5_f507bb53ac95f51e6a5a82867ddc5df3.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "ESAmod2-Sc1v5_n_export-v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\difference maps\ESAmod2-Sc2v5_n_export_md5_29ec1b644c1c1f745c73352eae079afa.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "ESAmod2-Sc2v5_n_export-v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\difference maps\ESAmod2-Sc1v5_n_export_md5_f507bb53ac95f51e6a5a82867ddc5df3.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "ESAmod2-Sc1v5_n_export-v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\difference maps\ESAmod2-Sc2v5_sed_export_md5_5095eba15fa6b590e3fe119ce2a01609.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "ESAmod2-Sc2v5_sed_export-v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\difference maps\ESAmod2-Sc2v6_n_export_md5_ec463d389a04f745bd818068d134e3af.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "ESAmod2-Sc2v6_n_export-v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\difference maps\ESAmod2-Sc2v6_sed_export_md5_f3845d38dd0acae5efa6ff04665e6612.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "ESAmod2-Sc2v6_sed_export-v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\difference maps\ESAmod2-Sc3v1_n_export_md5_dff4e429f760058e3addaf39f14ef833.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "ESAmod2-Sc3v1_n_export-v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\difference maps\ESAmod2-Sc3v1_sed_export_md5_561ca975a5f3b53d98d87bc085641954.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "ESAmod2-Sc3v1_sed_export-v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\difference maps\ESAmod2-Sc3v2_n_export_md5_f07dddc49c923b893a69a4480b3b1a6a.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "ESAmod2-Sc3v2_n_export-v2.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\difference maps\ESAmod2-Sc3v2_sed_export_md5_4874f6fe61fe3e1fd263501a77bcebb6.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "ESAmod2-Sc3v2_sed_export-v2.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': 'raster2 - raster1',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\ESAmodVCFv2_cv_habitat_value_md5_c01e9b17aee323ead79573d66fa4020d.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\Sc1v5_cv_habitat_value_md5_d93c19130503dd3f7ef51334cb7dfb4b.tif",
            },
            'target_nodata': -1e34,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "Sc1v5-ESAmod2_cv_habvalue.tif",
        },
        {
            'expression': 'raster2 - raster1',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\ESAmodVCFv2_cv_habitat_value_md5_c01e9b17aee323ead79573d66fa4020d.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\Sc1v6_cv_habitat_value_md5_ade277e546baa31877c0ce5ca6cb1ca2.tif",
            },
            'target_nodata': -1e34,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "Sc1v6-ESAmod2_cv_habvalue.tif",
        },
        {
            'expression': 'raster2 - raster1',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\ESAmodVCFv2_cv_habitat_value_md5_c01e9b17aee323ead79573d66fa4020d.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\Sc2v5_cv_habitat_value_md5_6b714f1ee115c0a958adbd3f4de8126e.tif",
            },
            'target_nodata': -1e34,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "Sc2v5-ESAmod2_cv_habvalue.tif",
        },
        {
            'expression': 'raster2 - raster1',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\ESAmodVCFv2_cv_habitat_value_md5_c01e9b17aee323ead79573d66fa4020d.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\Sc2v6_cv_habitat_value_md5_a14ded315c8e0ce2f2266a1c190e06ee.tif",
            },
            'target_nodata': -1e34,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "Sc2v6-ESAmod2_cv_habvalue.tif",
        },
        {
            'expression': 'raster2 - raster1',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\ESAmodVCFv2_cv_habitat_value_md5_c01e9b17aee323ead79573d66fa4020d.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\Sc3v1_cv_habitat_value_md5_e889c2dbc5783fc4c782fbd3b473d7de.tif",
            },
            'target_nodata': -1e34,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "Sc3v1-ESAmod2_cv_habvalue.tif",
        },
        {
            'expression': 'raster2 - raster1',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\ESAmodVCFv2_cv_habitat_value_md5_c01e9b17aee323ead79573d66fa4020d.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\Sc3v2_cv_habitat_value_md5_251c1c934b367c8181b873099d1118b8.tif",
            },
            'target_nodata': -1e34,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "Sc3v2-ESAmod2_cv_habvalue.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    #CV has to be handled differently because there are lots of data in the restored where there's nodata in the baseline (because we've added back habitat)
    #python add_sub_missing_as_0.py ESAmodVCFv2_cv_habitat_value_md5_c01e9b17aee323ead79573d66fa4020d.tif Sc1v5_cv_habitat_value_md5_d93c19130503dd3f7ef51334cb7dfb4b.tif --subtract
    #python add_sub_missing_as_0.py ESAmodVCFv2_cv_habitat_value_md5_c01e9b17aee323ead79573d66fa4020d.tif Sc2v5_cv_habitat_value_md5_6b714f1ee115c0a958adbd3f4de8126e.tif --subtract
    #python add_sub_missing_as_0.py ESAmodVCFv2_cv_habitat_value_md5_c01e9b17aee323ead79573d66fa4020d.tif Sc1v6_cv_habitat_value_md5_ade277e546baa31877c0ce5ca6cb1ca2.tif --subtract
    #python add_sub_missing_as_0.py ESAmodVCFv2_cv_habitat_value_md5_c01e9b17aee323ead79573d66fa4020d.tif Sc2v6_cv_habitat_value_md5_a14ded315c8e0ce2f2266a1c190e06ee.tif --subtract
    #python add_sub_missing_as_0.py ESAmodVCFv2_cv_habitat_value_md5_c01e9b17aee323ead79573d66fa4020d.tif Sc3v1_cv_habitat_value_md5_e889c2dbc5783fc4c782fbd3b473d7de.tif --subtract
    #python add_sub_missing_as_0.py ESAmodVCFv2_cv_habitat_value_md5_c01e9b17aee323ead79573d66fa4020d.tif Sc3v2_cv_habitat_value_md5_251c1c934b367c8181b873099d1118b8.tif --subtract



    calculation_list = [
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_n_export_esamod2_compressed_md5_96c12f4f833498771d18b131b8cbb49b.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_n_export_sc1v5renato_gt_0_5_compressed_md5_34c0d61416745062617bc2a981c2f4cc.tif",
            },
            'target_nodata': -1e34,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "ESAmod2-Sc1v5_n_export.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_n_export_esamod2_compressed_md5_96c12f4f833498771d18b131b8cbb49b.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_n_export_sc1v6renato_gt_0_001_compressed_md5_8da4c85909259d0ab5278bb7102f79ba.tif",
            },
            'target_nodata': -1e34,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "ESAmod2-Sc1v6_n_export.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_n_export_esamod2_compressed_md5_96c12f4f833498771d18b131b8cbb49b.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_n_export_sc2v5griscom2035_compressed_md5_ed32c23637a03c56a34a553956ddbffb.tif",
            },
            'target_nodata': -1e34,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "ESAmod2-Sc2v5_n_export.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_n_export_esamod2_compressed_md5_96c12f4f833498771d18b131b8cbb49b.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_n_export_sc2v6griscom2050_compressed_md5_6496502ed854d096a27966d0a3c5274c.tif",
            },
            'target_nodata': -1e34,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "ESAmod2-Sc2v6_n_export.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_n_export_esamod2_compressed_md5_96c12f4f833498771d18b131b8cbb49b.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_n_export_sc3v1pnvnoag_compressed_md5_bd5a856e0c1f76b2e8898f533ec20659.tif",
            },
            'target_nodata': -1e34,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "ESAmod2-Sc3v1_n_export.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_n_export_esamod2_compressed_md5_96c12f4f833498771d18b131b8cbb49b.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_n_export_sc3v2pnvall_compressed_md5_09bc65fe1cd54b518cde859f57513d8c.tif",
            },
            'target_nodata': -1e34,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "ESAmod2-Sc3v2_n_export.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_sed_export_esamod2_compressed_md5_fa10fd3d1942d0c3ce78b5aa544b150f.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_sed_export_sc1v5renato_gt_0_5_compressed_md5_11c71522df4068e22b1bb52864ed3c40.tif",
            },
            'target_nodata': -1e34,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "ESAmod2-Sc1v5_sed_export.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_sed_export_esamod2_compressed_md5_fa10fd3d1942d0c3ce78b5aa544b150f.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_sed_export_sc1v6renato_gt_0_001_compressed_md5_f7f21dc8e3a5689a9e3ab00e7fca93ed.tif",
            },
            'target_nodata': -1e34,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "ESAmod2-Sc1v6_sed_export.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_sed_export_esamod2_compressed_md5_fa10fd3d1942d0c3ce78b5aa544b150f.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_sed_export_sc2v5griscom2035_compressed_md5_424295c38fef2eff0c57ffe01ce9da71.tif",
            },
            'target_nodata': -1e34,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "ESAmod2-Sc2v5_sed_export.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_sed_export_esamod2_compressed_md5_fa10fd3d1942d0c3ce78b5aa544b150f.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_sed_export_sc2v6griscom2050_compressed_md5_dd086d47af09dc678d875a2845d68864.tif",
            },
            'target_nodata': -1e34,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "ESAmod2-Sc2v6_sed_export.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_sed_export_esamod2_compressed_md5_fa10fd3d1942d0c3ce78b5aa544b150f.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_sed_export_sc3v1pnvnoag_compressed_md5_2783ee50e908a763622d3167669b60bc.tif",
            },
            'target_nodata': -1e34,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "ESAmod2-Sc3v1_sed_export.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_sed_export_esamod2_compressed_md5_fa10fd3d1942d0c3ce78b5aa544b150f.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\3rdround_results\global_sed_export_sc3v2pnvall_compressed_md5_75dc87fbdb92f34efb987a3ba2c12d70.tif",
            },
            'target_nodata': -1e34,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "ESAmod2-Sc3v2_sed_export.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\scenarios\scenario_masks\reclassified_Sc1v5_md5_85604d25eb189f3566712feb506a8b9f_esa_to_nathab_forest_mask.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenarios\scenario_masks\reclassified_ESAmodVCFv2_md5_05407ed305c24604eb5a38551cddb031_esa_to_nathab_forest_mask.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "Sc1v5-ESAmod2.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\scenarios\scenario_masks\reclassified_Sc1v6_md5_c3539eae022a1bf588142bc363edf5a3_esa_to_nathab_forest_mask.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenarios\scenario_masks\reclassified_ESAmodVCFv2_md5_05407ed305c24604eb5a38551cddb031_esa_to_nathab_forest_mask.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "Sc1v6-ESAmod2.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\scenarios\scenario_masks\reclassified_Sc2v5_md5_a3ce41871b255adcd6e1c65abfb1ddd0_esa_to_nathab_forest_mask.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenarios\scenario_masks\reclassified_ESAmodVCFv2_md5_05407ed305c24604eb5a38551cddb031_esa_to_nathab_forest_mask.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "Sc2v5-ESAmod2.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\scenarios\scenario_masks\reclassified_Sc2v6_md5_dc75e27f0cb49a84e082a7467bd11214_esa_to_nathab_forest_mask.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenarios\scenario_masks\reclassified_ESAmodVCFv2_md5_05407ed305c24604eb5a38551cddb031_esa_to_nathab_forest_mask.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "Sc2v6-ESAmod2.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\scenarios\scenario_masks\reclassified_Sc3v1_PNVnoag_md5_c07865b995f9ab2236b8df0378f9206f_esa_to_nathab_forest_mask.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenarios\scenario_masks\reclassified_ESAmodVCFv2_md5_05407ed305c24604eb5a38551cddb031_esa_to_nathab_forest_mask.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "Sc3v1-ESAmod2.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\scenarios\scenario_masks\reclassified_Sc3v2_PNVallhab_md5_419ab9f579d10d9abb03635c5fdbc7ca_esa_to_nathab_forest_mask.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenarios\scenario_masks\reclassified_ESAmodVCFv2_md5_05407ed305c24604eb5a38551cddb031_esa_to_nathab_forest_mask.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "Sc3v2-ESAmod2.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    #starting over with scenarios, going to use table_reclass_by_threshold for both Scenarios 1 and 2, so need to convert MM's TC into probability of flip
    calculation_list = [
        {
            'expression': '(raster1<40)*(raster2<40)*(raster3<40) + (raster1<40)*(raster2>=40)*3 + (raster1<40)*(raster2<40)*(raster3>=40)*2 + (raster1>=40)',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\scenarios\tree_cover_MM\MulliganVCFTree1km.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenarios\tree_cover_MM\MulliganTC2035_Griscom_CookPatton.tif",
                'raster3': r"C:\Users\Becky\Documents\ci-global-restoration\scenarios\tree_cover_MM\MulliganTC2050_Griscom_CookPatton.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "MMGCP_1_nochange_2_2050_3_2035.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': 'raster1 + raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\Nbackgroundrates_smithpnv_md5_70ffbb628551efdf7b086de8258681fc.tif",
                'raster2': r"C:\Users\Becky\Documents\NfertratescurrentRevQ_md5_092d4fd6658df3073fd0c8d9c5a974cf.tif",
            },
            'target_nodata': -9999,
            'default_nan': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "Nrates_NCIcurrentRevQ_add_smithpnv_background.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': ('raster1 - raster2'),
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\ndr\global_n_export_esa_modVCFTree1km_compressed_md5_6d92706d1caa9c1b58aa41e503f13a36.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\ndr\global_n_export_Sc2v4_Griscom_CookPatton2035_smithpnv_compressed_md5_67a168b8617e6cd5dad5a1d8c737ea08.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "n_export_diff_ESA-Sc2v4_Griscom2035.tif",
        },
        {
            'expression': ('raster1 - raster2'),
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\ndr\global_n_export_esa_modVCFTree1km_compressed_md5_6d92706d1caa9c1b58aa41e503f13a36.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\ndr\global_n_export_Sc2v3_Griscom_CookPatton2050_smithpnv_compressed_md5_7c7e857ae29bc0f8e1aa0a1372888dbf.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "n_export_diff_ESA-Sc2v3_Griscom2050.tif",
        },
        {
            'expression': ('raster1 - raster2'),
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\ndr\global_n_export_esa_modVCFTree1km_compressed_md5_6d92706d1caa9c1b58aa41e503f13a36.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\ndr\global_n_export_Sc1v4_restoration_pnv0_001_on_ESA2020mVCF_compressed_md5_b8606e99a3328b325f67a8f6075cd60f.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "n_export_diff_ESA-Sc1v4_Renato0_001.tif",
        },
        {
            'expression': ('raster1 - raster2'),
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\ndr\global_n_export_esa_modVCFTree1km_compressed_md5_6d92706d1caa9c1b58aa41e503f13a36.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\ndr\global_n_export_Sc1v3_restoration_pnv0_5_on_ESA2020mVCF_compressed_md5_8effc0ab981d4691e59ba6fcaa00dd02.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "n_export_diff_ESA-Sc1v3_Renato0_5.tif",
        },
        {
            'expression': ('raster1 - raster2'),
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\sdr\global_sed_export_esa_modVCFTree1km_compressed_md5_d3ba34f5744f1104ce1ea598e7f7e526.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\sdr\global_sed_export_Sc2v4_Griscom_CookPatton2035_smithpnv_compressed_md5_7696e7b09b03cdfcf4994b5b2335aa05.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "sed_export_diff_ESA-Sc2v4_Griscom2035.tif",
        },
        {
            'expression': ('raster1 - raster2'),
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\sdr\global_sed_export_esa_modVCFTree1km_compressed_md5_d3ba34f5744f1104ce1ea598e7f7e526.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\sdr\global_sed_export_Sc2v3_Griscom_CookPatton2050_smithpnv_compressed_md5_e0629887851d09c22e8c69f5628b7960.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "sed_export_diff_ESA-Sc2v3_Griscom2050.tif",
        },
        {
            'expression': ('raster1 - raster2'),
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\sdr\global_sed_export_esa_modVCFTree1km_compressed_md5_d3ba34f5744f1104ce1ea598e7f7e526.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\sdr\global_sed_export_Sc1v4_restoration_pnv0_001_on_ESA2020mVCF_compressed_md5_158f5274a7a24bfe06add1ee818a4f9d.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "sed_export_diff_ESA-Sc1v4_Renato0_001.tif",
        },
        {
            'expression': ('raster1 - raster2'),
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\sdr\global_sed_export_esa_modVCFTree1km_compressed_md5_d3ba34f5744f1104ce1ea598e7f7e526.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\sdr\global_sed_export_Sc1v3_restoration_pnv0_5_on_ESA2020mVCF_compressed_md5_2ed88cee71891237eea41c44957c1cf2.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "sed_export_diff_ESA-Sc1v3_Renato0_5.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        #{
        #    'expression': ('raster1 - raster2'),
        #    'symbol_to_path_map': {
        #        'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\nature_access\nature_access_lspop2019_Sc1v3_Renato0_5_md5_3c8ebd6b98835a53ef29fa3019e9a369.tif",
        #        'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\nature_access\nature_access_lspop2019_esa2020modVCFhab_md5_a6519ebd8b941444921e749da2e645bb.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_raster_path': "nature_access_diff_Sc1v3-ESA_Renato0_5.tif",
        #},
        #{
        #    'expression': ('raster1 - raster2'),
        #    'symbol_to_path_map': {
        #        'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\nature_access\nature_access_lspop2019_Sc1v4_Renato0_001_md5_6558ab6f544caaa77dbe6ba74a9bfd6f.tif",
        #        'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\nature_access\nature_access_lspop2019_esa2020modVCFhab_md5_a6519ebd8b941444921e749da2e645bb.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_raster_path': "nature_access_diff_Sc1v4-ESA_Renato0_001.tif",
        #},
        #{
        #    'expression': ('raster1 - raster2'),
        #    'symbol_to_path_map': {
        #        'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\nature_access\nature_access_lspop2019_Sc2v3_Griscom2050_md5_2183e49010bc83b024bae12aae56fb3f.tif",
        #        'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\nature_access\nature_access_lspop2019_esa2020modVCFhab_md5_a6519ebd8b941444921e749da2e645bb.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_raster_path': "nature_access_diff_Sc2v3-ESA_Griscom2050.tif",
        #},
        #{
        #    'expression': ('raster1 - raster2'),
        #    'symbol_to_path_map': {
        #        'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\nature_access\nature_access_lspop2019_Sc2v4_Griscom2035_md5_f9fc25a6bb88bbba7f4530b25253675b.tif",
        #        'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\nature_access\nature_access_lspop2019_esa2020modVCFhab_md5_a6519ebd8b941444921e749da2e645bb.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_raster_path': "nature_access_diff_Sc2v4-ESA_Griscom2035.tif",
        #},
        #{
        #    'expression': ('raster1 - raster2'),
        #    'symbol_to_path_map': {
        #        'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\downstream_benes\downstream_benes_Sc1v3_Renato0_5_md5_57bd23fb0b2eab2de93814211d7c76db.tif",
        #        'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\downstream_benes\downstream_benes_esa2020modVCFhab_md5_0167a23792ceb1a65fa35ab92706ed30.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_raster_path': "downstream_benes_diff_Sc1v3-ESA_Renato0_5.tif",
        #},
        {
            'expression': ('raster1 - raster2'),
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\downstream_benes\downstream_benes_Sc1v4_Renato0_001_md5_c38a1f9035c87b256dc8a9e668127bfb.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\downstream_benes\downstream_benes_esa2020modVCFhab_md5_0167a23792ceb1a65fa35ab92706ed30.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "downstream_benes_diff_Sc1v4-ESA_Renato0_001.tif",
        },
        {
            'expression': ('raster1 - raster2'),
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\downstream_benes\downstream_benes_Sc2v3_Griscom2050_md5_08a398b06726b1139956158cd632e9a0.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\downstream_benes\downstream_benes_esa2020modVCFhab_md5_0167a23792ceb1a65fa35ab92706ed30.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "downstream_benes_diff_Sc2v3-ESA_Griscom2050.tif",
        },
        {
            'expression': ('raster1 - raster2'),
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\downstream_benes\downstream_benes_Sc2v4_Griscom2035_md5_73dae789315bf4300e920728125835cc.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\downstream_benes\downstream_benes_esa2020modVCFhab_md5_0167a23792ceb1a65fa35ab92706ed30.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "downstream_benes_diff_Sc2v4-ESA_Griscom2035.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': 'raster1*(raster2>1)',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\downstream_benes\esadownstream_bene_2019_50000.0_md5_b30c9cde883aa2f3dc9c4c4be265ea1a.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenario_nathab-forest-masks\reclassified_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46_esa_to_nathab_forest_mask.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "downstream_benes_esa2020modVCFhab.tif",
        },
        {
            'expression': 'raster1*(raster2>1)',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\downstream_benes\esadownstream_bene_2019_50000.0_md5_b30c9cde883aa2f3dc9c4c4be265ea1a.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenario_nathab-forest-masks\reclassified_Sc1v3_restoration_pnv0.5_on_ESA2020mVCF_md5_403f35b2a8b9b917090703e291f6bc0c_esa_to_nathab_forest_mask.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "downstream_benes_Sc1v3_Renato0_5.tif",
        },
        {
            'expression': 'raster1*(raster2>1)',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\downstream_benes\esadownstream_bene_2019_50000.0_md5_b30c9cde883aa2f3dc9c4c4be265ea1a.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenario_nathab-forest-masks\reclassified_Sc1v4_restoration_pnv0.001_on_ESA2020mVCF_md5_61a44df722532a84a77598fe2a24d46c_esa_to_nathab_forest_mask.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "downstream_benes_Sc1v4_Renato0_001.tif",
        },
        {
            'expression': 'raster1*(raster2>1)',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\downstream_benes\esadownstream_bene_2019_50000.0_md5_b30c9cde883aa2f3dc9c4c4be265ea1a.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenario_nathab-forest-masks\reclassified_Sc2v4_Griscom_CookPatton2035_smithpnv_md5_ffde2403583e30d7df4d16a0687d71fe_esa_to_nathab_forest_mask.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "downstream_benes_Sc2v4_Griscom2035.tif",
        },
        {
            'expression': 'raster1*(raster2>1)',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\downstream_benes\esadownstream_bene_2019_50000.0_md5_b30c9cde883aa2f3dc9c4c4be265ea1a.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenario_nathab-forest-masks\reclassified_Sc2v3_Griscom_CookPatton2050_smithpnv_md5_82c2f863d49f5a25c0b857865bfdb4b0_esa_to_nathab_forest_mask.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "downstream_benes_Sc2v3_Griscom2050.tif",
        },
        {
            'expression': 'raster1*(raster2>1)',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\nature_access\global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenario_nathab-forest-masks\reclassified_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_esa_to_nathab_forest_mask_WARPED_near_md5_b806ff033477b00c5886d79e7b92c485.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "nature_access_lspop2019_esa2020modVCFhab.tif",
        },
        {
            'expression': 'raster1*(raster2>1)',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\nature_access\global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenario_nathab-forest-masks\reclassified_Sc1v3_restoration_pnv0.5_on_ESA2020mVCF_esa_to_nathab_forest_mask_WARPED_near_md5_a5ba24a4bce2b1c217ffaa17a570ece4.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "nature_access_lspop2019_Sc1v3_Renato0_5.tif",
        },
        {
            'expression': 'raster1*(raster2>1)',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\nature_access\global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenario_nathab-forest-masks\reclassified_Sc1v4_restoration_pnv0.001_on_ESA2020mVCF_esa_to_nathab_forest_mask_WARPED_near_md5_bf2d507a4946072a5d6eb85295e5a71c.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "nature_access_lspop2019_Sc1v4_Renato0_001.tif",
        },
        {
            'expression': 'raster1*(raster2>1)',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\nature_access\global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenario_nathab-forest-masks\reclassified_Sc2v4_Griscom_CookPatton2035_smithpnv_esa_to_nathab_forest_mask_WARPED_near_md5_645d1740b81caa586f11f9921dfc4cb7.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "nature_access_lspop2019_Sc2v4_Griscom2035.tif",
        },
        {
            'expression': 'raster1*(raster2>1)',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\nature_access\global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenario_nathab-forest-masks\reclassified_Sc2v3_Griscom_CookPatton2050_smithpnv_esa_to_nathab_forest_mask_WARPED_near_md5_66a2ded74aa865c315f54e9008dc3806.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "nature_access_lspop2019_Sc2v3_Griscom2050.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return




    calculation_list = [
        {
            'expression': ('raster1 - raster2'),
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\scenario_nathab-forest-masks\reclassified_Sc1v3_restoration_pnv0.5_on_ESA2020mVCF_md5_403f35b2a8b9b917090703e291f6bc0c_esa_to_nathab_forest_mask.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenario_nathab-forest-masks\reclassified_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46_esa_to_nathab_forest_mask.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "Sc1v3-ESA.tif",
        },
        {
            'expression': ('raster1 - raster2'),
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\scenario_nathab-forest-masks\reclassified_Sc1v4_restoration_pnv0.001_on_ESA2020mVCF_md5_61a44df722532a84a77598fe2a24d46c_esa_to_nathab_forest_mask.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenario_nathab-forest-masks\reclassified_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46_esa_to_nathab_forest_mask.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "Sc1v4-ESA.tif",
        },
        {
            'expression': ('raster1 - raster2'),
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\scenario_nathab-forest-masks\reclassified_Sc2v3_Griscom_CookPatton2050_smithpnv_md5_82c2f863d49f5a25c0b857865bfdb4b0_esa_to_nathab_forest_mask.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenario_nathab-forest-masks\reclassified_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46_esa_to_nathab_forest_mask.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "Sc2v3-ESA.tif",
        },
        {
            'expression': ('raster1 - raster2'),
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\scenario_nathab-forest-masks\reclassified_Sc2v4_Griscom_CookPatton2035_smithpnv_md5_ffde2403583e30d7df4d16a0687d71fe_esa_to_nathab_forest_mask.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\scenario_nathab-forest-masks\reclassified_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46_esa_to_nathab_forest_mask.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "Sc2v4-ESA.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    ## The Renato scenarios were problematic in that they turned some places that were forest in ESA back to savanna. Should stomp original forest on.


    calculation_list = [
        {
            'expression': (
                '(raster1<190)*(raster2<41)*(raster3>40)*raster4 + (raster1>190)*(raster1<210)*(raster2<41)*(raster3>40)*raster4 + '
                '(raster1>189)*(raster1<191)*(raster2<41)*(raster3>40)*raster1 + (raster1>=210)*(raster2<41)*(raster3>40)*raster1 + '
                '(raster2>40)*raster1 + (raster2<41)*(raster3<41)*raster1'),
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\tree_cover_MM\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\tree_cover_MM\MulliganVCFTree1km.tif",
                'raster3': r"C:\Users\Becky\Documents\ci-global-restoration\tree_cover_MM\MulliganTC2050_Griscom_CookPatton.tif",
                'raster4': r"C:\Users\Becky\Documents\ci-global-restoration\PNV_all_ecosystems\PNV_smith_060420_md5_8dd464e0e23fefaaabe52e44aa296330.tif"
            },
            'target_nodata': 0,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "Sc2v3_Griscom_CookPatton2050_smithpnv.tif",
        },
        {
            'expression': (
                '(raster1<190)*(raster2<41)*(raster3>40)*raster4 + (raster1>190)*(raster1<210)*(raster2<41)*(raster3>40)*raster4 + '
                '(raster1>189)*(raster1<191)*(raster2<41)*(raster3>40)*raster1 + (raster1>=210)*(raster2<41)*(raster3>40)*raster1 + '
                '(raster2>40)*raster1 + (raster2<41)*(raster3<41)*raster1'),
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\tree_cover_MM\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46.tif",
                'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\tree_cover_MM\MulliganVCFTree1km.tif",
                'raster3': r"C:\Users\Becky\Documents\ci-global-restoration\tree_cover_MM\MulliganTC2035_Griscom_CookPatton.tif",
                'raster4': r"C:\Users\Becky\Documents\ci-global-restoration\PNV_all_ecosystems\PNV_smith_060420_md5_8dd464e0e23fefaaabe52e44aa296330.tif"
            },
            'target_nodata': 0,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "Sc2v4_Griscom_CookPatton2035_smithpnv.tif",
        },

    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    single_expression = { ##this was wrong - left nodata holes in urban and water where MM layers said it could reforest. Don't want those to change
        'expression': (
            '(raster1<190)*(raster2<41)*(raster3>40)*raster4 + (raster1>190)*(raster1<210)*(raster2<41)*(raster3>40)*raster4 + '
            '(raster2>40)*raster1 + (raster2<41)*(raster3<41)*raster1'),
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\tree_cover_MM\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46.tif",
            'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\tree_cover_MM\MulliganVCFTree1km.tif",
            'raster3': r"C:\Users\Becky\Documents\ci-global-restoration\tree_cover_MM\MulliganTC2035_Griscom_CookPatton.tif",
            'raster4': r"C:\Users\Becky\Documents\ci-global-restoration\PNV_all_ecosystems\PNV_smith_060420_md5_8dd464e0e23fefaaabe52e44aa296330.tif"
        },
        'target_nodata': 0,
        'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
        'resample_method': 'near',
        'target_raster_path': "Sc2v2_Griscom_CookPatton2035_smithpnv.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = { #these were wrong too
        'expression': (
            '(raster1<190)*(raster2<41)*(raster3>40)*raster4 + (raster1>190)*(raster1<210)*(raster2<41)*(raster3>40)*raster4 + '
            '(raster2>40)*raster1 + (raster2<41)*(raster3<41)*raster1'),
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\tree_cover_MM\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46.tif",
            'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\tree_cover_MM\MulliganVCFTree1km.tif",
            'raster3': r"C:\Users\Becky\Documents\ci-global-restoration\tree_cover_MM\MulliganTC2050_Griscom_CookPatton.tif",
            'raster4': r"C:\Users\Becky\Documents\ci-global-restoration\PNV_all_ecosystems\PNV_smith_060420_md5_8dd464e0e23fefaaabe52e44aa296330.tif"
        },
        'target_nodata': 0,
        'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
        'resample_method': 'near',
        'target_raster_path': "Sc2_Griscom_CookPatton_smithpnv.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': (
            '(raster1<190)*(raster2<41)*(raster3>40)*51 + (raster1>190)*(raster1<210)*(raster2<41)*(raster3>40)*51 + '
            '(raster2>40)*raster1 + (raster2<41)*(raster3<41)*raster1'),
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\tree_cover_MM\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46.tif",
            'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\tree_cover_MM\MulliganVCFTree1km.tif",
            'raster3': r"C:\Users\Becky\Documents\ci-global-restoration\tree_cover_MM\MulliganTC2050_Griscom_CookPatton.tif",
        },
        'target_nodata': 0,
        'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
        'resample_method': 'near',
        'target_raster_path': "Sc2_Griscom_CookPatton.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': (
            '(raster1<51)*(raster1>49)*(raster2>40)*51 + (raster1<51)*(raster1>49)*(raster2<41)*52 + '
            '(raster1<61)*(raster1>59)*(raster2>40)*61 + (raster1<61)*(raster1>59)*(raster2<41)*62 + '
            '(raster1<71)*(raster1>69)*(raster2>40)*71 + (raster1<71)*(raster1>69)*(raster2<41)*72 + '
            '(raster1<81)*(raster1>79)*(raster2>40)*81 + (raster1<81)*(raster1>79)*(raster2<41)*82 + '
            '(raster1<91)*(raster1>89)*(raster2>40)*91 + (raster1<91)*(raster1>89)*(raster2<41)*92 + '
            '(raster1>60)*(raster1<63)*raster1 + (raster1>70)*(raster1<73)*raster1 + (raster1>80)*(raster1<83)*raster1 + '
            '(raster1<50)*raster1 + (raster1>90)*raster1'),
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_2ed6285e6f8ec1e7e0b75309cc6d6f9f.tif",
            'raster2':r"C:\Users\Becky\Documents\ci-global-restoration\tree_cover_MM\MulliganVCFTree1km.tif",
        },
        'target_nodata': 0,
        'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
        'resample_method': 'near',
        'target_raster_path': "ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km1.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [ #these didn't work - produced a lot of 241 values
        {
           'expression': '(raster1<=51)*(raster1>49) + (raster1<=61)*(raster1>59) + (raster1<=71)*(raster1>69) + (raster1<=81)*(raster1>79) + (raster1<=91)*(raster1>89) + (raster1>=160)*(raster1<=170)',
           'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\Sc1v2_restoration_pnv0.0001_on_ESA2020_v2_md5_47613f8e4d340c92b2c481cc8080cc9d.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "Sc1v2_restoration_pnv0.0001_on_ESA2020_v2_md5_47613f8e4d340c92b2c481cc8080cc9d_forest_mask.tif",
        },
        {
           'expression': '(raster1<=51)*(raster1>49) + (raster1<=61)*(raster1>59) + (raster1<=71)*(raster1>69) + (raster1<=81)*(raster1>79) + (raster1<=91)*(raster1>89) + (raster1>=160)*(raster1<=170)',
           'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\Sc1v3_restoration_pnv0.5_on_ESA2020mVCF_md5_403f35b2a8b9b917090703e291f6bc0c.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "Sc1v3_restoration_pnv0.5_on_ESA2020mVCF_md5_403f35b2a8b9b917090703e291f6bc0c_forest_mask.tif",
        },
        {
           'expression': '(raster1<=51)*(raster1>49) + (raster1<=61)*(raster1>59) + (raster1<=71)*(raster1>69) + (raster1<=81)*(raster1>79) + (raster1<=91)*(raster1>89) + (raster1>=160)*(raster1<=170)',
           'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\Sc2_Griscom_CookPatton_smithpnv_md5_1536327d82e292529e7872dc6ecc2871.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "Sc2_Griscom_CookPatton_smithpnv_md5_1536327d82e292529e7872dc6ecc2871_forest_mask.tif",
        },
        {
           'expression': '(raster1<=51)*(raster1>49) + (raster1<=61)*(raster1>59) + (raster1<=71)*(raster1>69) + (raster1<=81)*(raster1>79) + (raster1<=91)*(raster1>89) + (raster1>=160)*(raster1<=170)',
           'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\Sc2_Griscom_CookPatton_md5_21ad308d97dd1c6f676fc7fc7004f0b9.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "Sc2_Griscom_CookPatton_md5_21ad308d97dd1c6f676fc7fc7004f0b9_forest_mask.tif",
        },
        {
           'expression': '(raster1<=51)*(raster1>49) + (raster1<=61)*(raster1>59) + (raster1<=71)*(raster1>69) + (raster1<=81)*(raster1>79) + (raster1<=91)*(raster1>89) + (raster1>=160)*(raster1<=170)',
           'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\tree_cover_MM\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46_forest_mask.tif",
        },
        {
           'expression': '(raster1<=51)*(raster1>49) + (raster1<=61)*(raster1>59) + (raster1<=71)*(raster1>69) + (raster1<=81)*(raster1>79) + (raster1<=91)*(raster1>89) + (raster1>=160)*(raster1<=170)',
           'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_2ed6285e6f8ec1e7e0b75309cc6d6f9f.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_2ed6285e6f8ec1e7e0b75309cc6d6f9f_forest_mask.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    calculation_list = [
        {
           'expression': '(raster1>0)*raster2',
           'symbol_to_path_map': {
               'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\pollination\workspace_poll_suff_ESA2020\churn\hab_mask\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_2ed6285e6f8ec1e7e0b75309cc6d6f9f_hab_mask.tif",
               'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\downstream_benes\esadownstream_bene_2019_50000.0_md5_b30c9cde883aa2f3dc9c4c4be265ea1a.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "downstream_benes_esa2020hab.tif",
        },
        {
           'expression': '(raster1>0)*raster2',
           'symbol_to_path_map': {
               'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\pollination\workspace_poll_suff_Scenario1v2\churn\hab_mask\restoration_pnv0.0001_on_ESA2020_v2_md5_47613f8e4d340c92b2c481cc8080cc9d_hab_mask.tif",
               'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\downstream_benes\esadownstream_bene_2019_50000.0_md5_b30c9cde883aa2f3dc9c4c4be265ea1a.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "downstream_benes_Sc1v2hab.tif",
        },
        #{
        #   'expression': 'raster1-raster2',
        #   'symbol_to_path_map': {
        #       'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\pollination\workspace_poll_suff_Scenario1v2\churn\hab_mask\restoration_pnv0.0001_on_ESA2020_v2_md5_47613f8e4d340c92b2c481cc8080cc9d_hab_mask.tif",
        #       'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\pollination\workspace_poll_suff_ESA2020\churn\hab_mask\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_2ed6285e6f8ec1e7e0b75309cc6d6f9f_hab_mask.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
        #    'resample_method': 'near',
        #    'target_raster_path': "habitat_sc1v2-esa2020.tif",
        #},
        #{
        #   'expression': 'raster1-raster2',
        #   'symbol_to_path_map': {
        #       'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\cv\coastal_risk_reduction_biophysical_value_Sc1v2_md5_900bbda6339d2320f2dcc3703a81d903.tif",
        #       'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\cv\coastal_risk_reduction_biophysical_value_esa2020_md5_25be26503800fb9d77e574ddaf5fefca.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
        #    'resample_method': 'near',
        #    'target_raster_path': "coastal_sc1v2-esa2020.tif",
        #},
        #{
        #   'expression': 'raster1-raster2',
        #   'symbol_to_path_map': {
        #       'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\nature_access\global_people_access_population_2019_1hr_Sc1hab_md5_19f1ceb9e10bd580471659fd85cdcece.tif",
        #       'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\nature_access\global_people_access_population_2019_1hr_ESA2020hab_md5_0f20c7ab86fa0738ce61e3d5fb4f142f.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_raster_path': "nature_access_sc1v2-esa2020.tif",
        #},
        #{
        #   'expression': 'raster1-raster2',
        #   'symbol_to_path_map': {
        #       'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\pollination\pollination_ppl_fed_on_ag_10s_restorationSc1v2_md5_28cb07a592e79eb2fd70d781945582e4.tif",
        #       'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\pollination\pollination_ppl_fed_on_ag_10s_esa2020_md5_0cf9025ab3a00691f29de359e590cf74.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_raster_path': "pollination_pplfedonag_sc1v2-esa2020.tif",
        #},
        #{
        #   'expression': 'raster1-raster2',
        #   'symbol_to_path_map': {
        #       'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\sdr\global_sed_export_compressed_md5_4b796b51bf1b4fb197fa30e35524d00e.tif",
        #       'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\sdr\global_sed_export_scenario_1_v2_lulc_compressed_md5_bbc42e0a88f2ca90f82f04e927ccd7e8.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_raster_path': "sediment-export_esa2020-sc1v2.tif",
        #},
        #{
        #   'expression': 'raster1-raster2',
        #   'symbol_to_path_map': {
        #       'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\ndr\global_n_export_esa_lulc_md5_9802273386b16d6ecb10764f7b382367_compressed.tif",
        #       'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\ndr\global_n_export_scenario_1_v2_lulc_compressed_md5_141a49f3c7eb8080d8f308ef9a3bf319.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_raster_path': "nitrogen-export_esa2020-sc1v2.tif",
        #}
        #this is actually a bad way to do it because the service decreases because there's less bad thing to mitigate
        #{
        #   'expression': 'raster1-raster2',
        #   'symbol_to_path_map': {
        #       'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\sdr\global_sed_deposition_scenario_1_v2_lulc_compressed_md5_32c8425b84aaaa3803c1da8c6b968471.tif",
        #       'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\sdr\global_sed_deposition_compressed_md5_5283cd43fd4ba1841ab4e326debeb7b1.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_raster_path': "sediment_sc1v2-esa2020.tif",
        #},
        #{
        #   'expression': 'raster1-raster2',
        #   'symbol_to_path_map': {
        #       'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\ndr\global_n_retention_scenario_1_v2_lulc_compressed_md5_93d3b14e77e5f18546c15fc20bd27e4c.tif",
        #       'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\ndr\global_n_retention_esa_lulc_md5_f3f740a927fda2157f97e82b4fafdbe7_compressed.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_raster_path': "nitrogen_sc1v2-esa2020.tif",
        #},
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
           'expression': '(raster1>0)*raster2',
           'symbol_to_path_map': {
               'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\nature_access\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_hab_mask_WARPED_near_md5_c09ceaf658819a1deb9fa5a8f11b6774.tif",
               'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\nature_access\global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "global_people_access_population_2019_1hr_ESA2020hab_Eckert.tif",
        },
        {
           'expression': '(raster1>0)*raster2',
           'symbol_to_path_map': {
               'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\nature_access\restoration_pnv0.0001_on_ESA2020_v2_hab_mask_WARPED_near_md5_f53c2305d123b90a01fe4e80b7dea7fe.tif",
               'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\nature_access\global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "global_people_access_population_2019_1hr_Sc1v2hab_Eckert.tif",
        },
        #{
        #   'expression': '(raster1>0)*raster2',
        #   'symbol_to_path_map': {
        #       'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\pollination\workspace_poll_suff_ESA2020\churn\hab_mask\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_2ed6285e6f8ec1e7e0b75309cc6d6f9f_hab_mask.tif",
        #       'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\ndr\global_n_retention_esa_lulc_md5_f3f740a927fda2157f97e82b4fafdbe7_compressed.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
        #    'resample_method': 'near',
        #    'target_raster_path': "n_retention_esa2020hab.tif",
        #},
        #{
        #   'expression': '(raster1>0)*raster2',
        #   'symbol_to_path_map': {
        #       'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\pollination\workspace_poll_suff_Scenario1v2\churn\hab_mask\restoration_pnv0.0001_on_ESA2020_v2_md5_47613f8e4d340c92b2c481cc8080cc9d_hab_mask.tif",
        #       'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\ndr\global_n_retention_scenario_1_v2_lulc_compressed_md5_93d3b14e77e5f18546c15fc20bd27e4c.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
        #    'resample_method': 'near',
        #    'target_raster_path': "n_retention_Sc1v2hab.tif",
        #},
        #{
        #   'expression': '(raster1>0)*raster2',
        #   'symbol_to_path_map': {
        #       'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\pollination\workspace_poll_suff_ESA2020\churn\hab_mask\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_2ed6285e6f8ec1e7e0b75309cc6d6f9f_hab_mask.tif",
        #       'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\sdr\global_sed_deposition_compressed_md5_5283cd43fd4ba1841ab4e326debeb7b1.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
        #    'resample_method': 'near',
        #    'target_raster_path': "sed_deposition_esa2020hab.tif",
        #},
        #{
        #   'expression': '(raster1>0)*raster2',
        #   'symbol_to_path_map': {
        #       'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\pollination\workspace_poll_suff_Scenario1v2\churn\hab_mask\restoration_pnv0.0001_on_ESA2020_v2_md5_47613f8e4d340c92b2c481cc8080cc9d_hab_mask.tif",
        #       'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\sdr\global_sed_deposition_scenario_1_v2_lulc_compressed_md5_32c8425b84aaaa3803c1da8c6b968471.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
        #    'resample_method': 'near',
        #    'target_raster_path': "sed_deposition_Sc1v2hab.tif",
        #},
        #{ #not sure why I felt like I needed to clip sediment and nitrogen.... so it would run faster? not going to repeat this
        #   'expression': '(raster1>=0)*raster2',
        #   'symbol_to_path_map': {
        #       'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\pnv_bin_10s_md5_2ad7053bdd41bbac732fd0ff943348ae.tif",
        #       'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\sdr\global_sed_deposition_scenario_1_lulc_compressed_md5_5dc786cac84645cda59e7fc43eba8d69.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
        #    'resample_method': 'near',
        #    'target_raster_path': "sediment_sc1_clipped_to_pnv_extent.tif",
        #},
        #{
        #   'expression': '(raster1>=0)*raster2',
        #   'symbol_to_path_map': {
        #       'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\pnv_bin_10s_md5_2ad7053bdd41bbac732fd0ff943348ae.tif",
        #       'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\ndr\global_n_retention_scenario_1_lulc_md5_4e9ddd38979d3d6a115f5cb826e469ea_compressed.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
        #    'resample_method': 'near',
        #    'target_raster_path': "nitrogen_sc1_clipped_to_pnv_extent.tif",
        #},
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': '(raster1>0)*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\original_rasters\cnc_cv\normalized_pop_on_hab\total_pop_masked_by_10m_md5_ef02b7ee48fa100f877e3a1671564be2.tif",
            'raster2': r"C:\Users\Becky\Documents\raster_calculations\lspop2019_compressed_md5_d0bf03bd0a2378196327bbe6e898b70c.tif",
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_pixel_size': (0.0083333333333333,-0.0083333333333333),
        'resample_method': 'near',
        'target_raster_path': "total_pop_masked_by_10m_2019.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return




    #first ran poll_suff with the following docker commands
    #docker run -d --name pollsuff_container --rm -v %CD%:/usr/local/workspace therealspring/inspring:latest make_poll_suff.py ./ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_2ed6285e6f8ec1e7e0b75309cc6d6f9f.tif && docker logs pollsuff_container -f
    #docker run -d --name pollsuff_container --rm -v %CD%:/usr/local/workspace therealspring/inspring:latest make_poll_suff.py ./restoration_pnv0.0001_on_ESA2020_compressed_md5_93d43b6124c73cb5dc21698ea5f9c8f4.tif && docker logs pollsuff_container -f
    #it doesn't actually take that long, a few hours on my laptop maybe?

    calculation_list = [
        {
           'expression': 'raster1*raster2*raster3*(raster4>0)+(raster4<1)*-9999',
           'symbol_to_path_map': {
               'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\pollination\monfreda_2008_yield_poll_dep_ppl_fed_5min.tif",
               'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\pollination\workspace_poll_suff_ESA2020\churn\poll_suff_hab_ag_coverage_rasters\poll_suff_ag_coverage_prop_10s_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_2ed6285e6f8ec1e7e0b75309cc6d6f9f.tif",
               'raster3': r"C:\Users\Becky\Documents\esa_pixel_area_ha_md5_1dd3298a7c4d25c891a11e01868b5db6.tif",
               'raster4': r"C:\Users\Becky\Documents\ci-global-restoration\pollination\workspace_poll_suff_ESA2020\churn\ag_mask\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_2ed6285e6f8ec1e7e0b75309cc6d6f9f_ag_mask.tif"
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "pollination_ppl_fed_on_ag_10s_esa2020.tif",
        },
        {
           'expression': 'raster1*raster2*raster3*(raster4>0)+(raster4<1)*-9999',
           'symbol_to_path_map': {
               'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\pollination\monfreda_2008_yield_poll_dep_ppl_fed_5min.tif",
               'raster2': r"C:\Users\Becky\Documents\ci-global-restoration\pollination\workspace_poll_suff_Scenario1v2\churn\poll_suff_hab_ag_coverage_rasters\poll_suff_ag_coverage_prop_10s_restoration_pnv0.0001_on_ESA2020_v2_md5_47613f8e4d340c92b2c481cc8080cc9d.tif",
               'raster3': r"C:\Users\Becky\Documents\esa_pixel_area_ha_md5_1dd3298a7c4d25c891a11e01868b5db6.tif",
               'raster4': r"C:\Users\Becky\Documents\ci-global-restoration\pollination\workspace_poll_suff_Scenario1v2\churn\ag_mask\restoration_pnv0.0001_on_ESA2020_v2_md5_47613f8e4d340c92b2c481cc8080cc9d_ag_mask.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "pollination_ppl_fed_on_ag_10s_restorationSc1v2.tif",
        },

    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    #then back to docker with both those layers:
    # docker run -d --name pollination_container --rm -v %CD%:/usr/local/workspace therealspring/inspring:latest realized_pollination.py pollination_ppl_fed_on_ag_10s_esa2020.tif && docker logs pollination_container -f
    # docker run -d --name pollination_container --rm -v %CD%:/usr/local/workspace therealspring/inspring:latest realized_pollination.py pollination_ppl_fed_on_ag_10s_restorationSc1.tif && docker logs pollination_container -f



#dont even need this, create_scenario.py is easier!
    calculation_list = [
        {
            'expression': '(raster1<=0)*raster2 + (raster1>0)*raster3',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\pnv_bin_10s_md5_2ad7053bdd41bbac732fd0ff943348ae.tif",
                'raster2': r"C:\Users\Becky\Documents\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_2ed6285e6f8ec1e7e0b75309cc6d6f9f.tif",
                'raster3': r"C:\Users\Becky\Documents\nci\scenarios\scenarios0221_restoration_md5_16450b43f0a232b32a847c9738affda3.tif"
            },
            'target_nodata': -1,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "restored_pnv_bin_10s_ESA2020_clip.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return




if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(WORKSPACE_DIR, NCPUS, 5.0)
    main()
