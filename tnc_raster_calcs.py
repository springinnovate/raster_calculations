"""These calculations are for the TNC NbS project."""
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
    # python add_sub_missing_as_0.py "D:\Users\richp\Downloads\nature_access_lspop2019_ESA2020ag_compressed_md5_6496bd.tif" "D:\repositories\tnc-sci-ncscobenefits\nature_access_lspop2019_afc2_compressed_md5_73d45b.tif" --subtract
    # python add_sub_missing_as_0.py "D:\repositories\tnc-sci-ncscobenefits\upload\nature_access_lspop2019_reforest2_compressed_md5_d31ae6.tif" "D:\Users\richp\Downloads\nature_access_lspop2019_ESA2020ag_compressed_md5_6496bd.tif" --subtract
    #python add_sub_missing_as_0.py "D:\repositories\tnc-sci-ncscobenefits\upload\nature_access_avg_lspop2019_reforest2_compressed_md5_5f7de4.tif" "D:\Users\richp\Downloads\nature_access_lspop2019_ESA2020ag_compressed_md5_6496bd.tif" --subtract
    #python add_sub_missing_as_0.py "D:\Users\richp\Downloads\nature_access_lspop2019_ESA2020ag_compressed_md5_6496bd.tif" "D:\repositories\tnc-sci-ncscobenefits\upload\nature_access_avg_lspop2019_afc2_compressed_md5_f67b6d.tif" --subtract

    calculation_list = [
        #{
        #    'expression': 'raster1*0.1',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\repositories\tnc-sci-ncscobenefits\carbon_data\aboveground_biomass_carbon_2010_md5_4be351.tif",
        #    },
        #    'target_nodata': 0,
        #    'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\carbon_data\carbon_v2_baseline_Mg_C_ha_AGB_spawn.tif",
        #},
        {
            'expression': 'raster1+raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\carbon_data\carbon_v2_baseline_Mg_C_ha_AGB_spawn_compressed_md5_5f64b4.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\carbon_results_v2\NBSdiff_carbon_v2_agroforest_md5_e55019.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.00277777778,-0.00277777778),
            'resample_method': 'near',
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\carbon_results_v2\carbon_v2_agroforest_2050.tif",
        },
        {
            'expression': 'raster1+raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\carbon_data\carbon_v2_baseline_Mg_C_ha_AGB_spawn_compressed_md5_5f64b4.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\carbon_results_v2\NBSdiff_carbon_v2_reforest2_md5_e72214.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.00277777778,-0.00277777778),
            'resample_method': 'near',
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\carbon_results_v2\carbon_v2_reforest2_2050.tif",
        },
        {
            'expression': 'raster1-raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\carbon_data\carbon_v2_baseline_Mg_C_ha_AGB_spawn_compressed_md5_5f64b4.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\carbon_results_v2\NBSdiff_carbon_v2_afc2_md5_4d53f0.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.00277777778,-0.00277777778),
            'resample_method': 'near',
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\carbon_results_v2\carbon_v2_afc2_2050.tif",
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
            'expression': 'raster1*raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\raster_calculations\align_to_mask_workspace\hab_mask_tnc_nbs_reforest2_WARPED_average_md5_442d33e524fc3d2eecf70c279e31f20e.tif",
                'raster2': r"D:\repositories\raster_calculations\align_to_mask_workspace\ecoshards\global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\nature_access_avg_lspop2019_reforest2.tif",
        },
        {
            'expression': 'raster1*raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\raster_calculations\align_to_mask_workspace\hab_mask_tnc_nbs_afc2_WARPED_average_md5_3ba406edbd1147eae1e2b6fe59a47cc3.tif",
                'raster2': r"D:\repositories\raster_calculations\align_to_mask_workspace\ecoshards\global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\nature_access_avg_lspop2019_afc2.tif",
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
            'expression': 'raster1*raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\raster_calculations\align_to_mask_workspace\hab_mask_tnc_nbs_reforest2_WARPED_near_md5_dacc59cb224c9be3163d2a1002935d4b.tif",
                'raster2': r"D:\repositories\raster_calculations\align_to_mask_workspace\ecoshards\global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\nature_access_lspop2019_reforest2.tif",
        },
        {
            'expression': 'raster1*raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\raster_calculations\align_to_mask_workspace\hab_mask_tnc_nbs_afc2_WARPED_near_md5_35e69b8d57b355dc126a2ff136016654.tif",
                'raster2': r"D:\repositories\raster_calculations\align_to_mask_workspace\ecoshards\global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\nature_access_lspop2019_afc2.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    calculation_list = [ #This raster seems to be a problem with nans that raster calcs can't handle. This didn't work.>> RICH FIXED IT
        {
            'expression': 'raster1*30',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\carbon_data\sequestration_rate__mean__aboveground__full_extent__Mg_C_ha_yr.tif",
            },
            'target_nodata': 0,
            'default_nan': 0,
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\carbon_results_v2\carbon_seq_30yrs_Mg_C_ha.tif",
        },
        {
            'expression': 'raster1*30*0.1',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\carbon_data\sequestration_rate__mean__aboveground__full_extent__Mg_C_ha_yr.tif",
            },
            'target_nodata': 0,
            'default_nan': 0,
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\carbon_results_v2\carbon_seq_30yrs_10pct_Mg_C_ha.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [ #This is what I was originally trying to do and it didn't work so I took just the first step (above) and even that didn't work
        {
            'expression': '(raster1>0)*raster2*30',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\DIFF_reforest2-ESA.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\carbon_data\sequestration_rate__mean__aboveground__full_extent__Mg_C_ha_yr.tif",
            },
            'target_nodata': 0,
            'default_nan': 0,
            'target_pixel_size': (0.00277777778,-0.00277777778),
            'resample_method': 'near',
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\carbon_results_v2\NBSdiff_reforest2_carbon_seq_Mg_C_ha.tif",
        },
        {
            'expression': '(raster1>0)*raster2*30*0.1',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\DIFF_agroforest-ESA.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\carbon_data\sequestration_rate__mean__aboveground__full_extent__Mg_C_ha_yr.tif",
            },
            'target_nodata': 0,
            'default_nan': 0,
            'target_pixel_size': (0.00277777778,-0.00277777778),
            'resample_method': 'near',
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\carbon_results_v2\NBSdiff_agroforest_carbon_seq_Mg_C_ha.tif",
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
            'expression': '(raster1>0)*0.1*raster2', #multiply by 0.1 because raster2 has weird scaling - see https://daac.ornl.gov/VEGETATION/guides/Global_Maps_C_Density_2010.html
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\DIFF_afc2-ESA.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\carbon_data\aboveground_biomass_carbon_2010_md5_4be351.tif",
            },
            'target_nodata': 0,
            'default_nan': 0,
            'target_pixel_size': (0.00277777778,-0.00277777778),
            'resample_method': 'near',
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\carbon_results_v2\NBSdiff_afc2_carbon_Mg_C_ha.tif",
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
            'expression': '(raster1)-(raster2)',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\TNC_NBS\mangroves_restore_2050_WARPED_near_md5_c88042.tif",
                'raster2': r"D:\ecoshard\TNC_NBS\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da.tif"
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\ecoshard\TNC_NBS\DIFFlulc_mangroverestf-2020.tif",
        },
        {
            'expression': '(raster1)-(raster2)',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\TNC_NBS\agroforestry_2050_md5_e32698.tif",
                'raster2': r"D:\ecoshard\TNC_NBS\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da.tif"
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\ecoshard\TNC_NBS\DIFFlulc_agroforest-2020.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()

    calculation_list = [ #reforest and afc were done below
        {
            'expression': '((raster1>0)+(raster1<0))*raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\TNC_NBS\DIFFlulc_agroforest-2020.tif",
                'raster2': r"D:\ecoshard\TNC_NBS\results\NBSdiff_cv_habitat_value_agroforest_compressed_md5_a16158.tif"
            },
            'target_nodata': 0,
            'target_pixel_size': (0.00277777778,-0.00277777778),
            'resample_method': 'near',
            'target_raster_path': r"D:\ecoshard\TNC_NBS\results\NBSdiff_cv_habitat_value_agroforest.tif",
        },
        {
            'expression': '((raster1>0)+(raster1<0))*raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\TNC_NBS\DIFFlulc_mangroverestf-2020.tif",
                'raster2': r"D:\ecoshard\TNC_NBS\results\NBSdiff_cv_habitat_value_mangroverestf_compressed_md5_f8e2b4.tif"
            },
            'target_nodata': 0,
            'target_pixel_size': (0.00277777778,-0.00277777778),
            'resample_method': 'near',
            'target_raster_path': r"D:\ecoshard\TNC_NBS\results\NBSdiff_cv_habitat_value_mangroverestf.tif",
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
                'raster1': r"D:\ecoshard\TNC_NBS\results\total_carbon_spawn_agroforest_compressed_md5_4430b1.tif",
                'raster2': r"D:\ecoshard\TNC_NBS\results\total_carbon_spawn_marESA2020_compressed_md5_bd0428.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\ecoshard\TNC_NBS\results\upload_these\NBSdiff_carbon_agroforest-2020.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\TNC_NBS\results\total_carbon_spawn_mangroverestf_compressed_md5_c6012b.tif",
                'raster2': r"D:\ecoshard\TNC_NBS\results\total_carbon_spawn_marESA2020_compressed_md5_bd0428.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\ecoshard\TNC_NBS\results\upload_these\NBSdiff_carbon_mangroverestf-2020.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_sed_export_marine_mod_ESA_2020_compressed_md5_a988c0.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_sed_export_tnc_nbs_agroforest_compressed_md5_91e77b.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\NBSdiff_sed_export_agroforest.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\ndr_sdr_global\workspace\global_sed_export_tnc_esa2020.tif",
                'raster2': r"D:\repositories\ndr_sdr_global\workspace\global_sed_export_tnc_nbs_mangroverest_f.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\ndr_sdr_global\workspace\NBSdiff_sed_export_mangroverestf.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\ndr_sdr_global\workspace\global_n_export_tnc_esa2020.tif",
                'raster2': r"D:\repositories\ndr_sdr_global\workspace\global_n_export_tnc_nbs_agroforest.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\ndr_sdr_global\workspace\NBSdiff_n_export_2020-agroforest.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\ndr_sdr_global\workspace\global_n_export_tnc_esa2020.tif",
                'raster2': r"D:\repositories\ndr_sdr_global\workspace\global_n_export_tnc_nbs_mangroverest_f.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\ndr_sdr_global\workspace\NBSdiff_n_export_2020-mangroverestf.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\ndr_sdr_global\workspace\global_n_export_tnc_esa2020.tif",
                'raster2': r"D:\repositories\ndr_sdr_global\workspace\global_n_export_tnc_nbs_reforest.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\ndr_sdr_global\workspace\NBSdiff_n_export_2020-reforest.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\ndr_sdr_global\workspace\global_n_export_tnc_nbs_afc.tif",
                'raster2': r"D:\repositories\ndr_sdr_global\workspace\global_n_export_tnc_esa2020.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\ndr_sdr_global\workspace\NBSdiff_n_export_afc-2020.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\results\total_carbon_spawn_reforest_compressed_md5_159cd2.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\results\total_carbon_spawn_marESA2020_compressed_md5_bd0428.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
            'resample_method': 'near',
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\results\NBSdiff_carbon_reforest.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\results\total_carbon_spawn_marESA2020_compressed_md5_bd0428.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\results\total_carbon_spawn_afc.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
            'resample_method': 'near',
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\results\NBSdiff_carbon_afc.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

# python reclassify_by_table_copied_from_costaricasdr.py  D:\repositories\tnc-sci-ncscobenefits\Ecoregions2017_reforest_md5_e2c9c4.tif D:\ecoshard\SpawnESA2010_Carbon_Lookup_Table_md5_f09bca.csv lucode 2010Mean
        #renamed to total_carbon_spawn_reforest
# python reclassify_by_table_copied_from_costaricasdr.py  D:\repositories\tnc-sci-ncscobenefits\Ecoregions2017_afc_md5_07db6d.tif D:\repositories\tnc-sci-ncscobenefits\SpawnESA2010_Carbon_Lookup_Table_md5_f09bca.csv lucode 2010Mean
        ##renamed to total_carbon_spawn_afc
#python reclassify_by_table_copied_from_costaricasdr.py  D:\repositories\tnc-sci-ncscobenefits\carbon_data\Ecoregions2017_agroforest_md5_e13d9a.tif D:\repositories\tnc-sci-ncscobenefits\carbon_data\SpawnESA2010_Carbon_Lookup_Table_md5_af3024.csv lucode 2010Mean
     ##renamed to total_carbon_spawn_agroforest
#python reclassify_by_table_copied_from_costaricasdr.py  D:\repositories\tnc-sci-ncscobenefits\carbon_data\Ecoregions2017_mangroverestf_md5_d9e1ea.tif D:\repositories\tnc-sci-ncscobenefits\carbon_data\SpawnESA2010_Carbon_Lookup_Table_md5_af3024.csv lucode 2010Mean
     ##renamed to total_carbon_spawn_mangroverestf

    calculation_list = [
        #{
        #    'expression': 'raster1 + (raster2*220)',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\repositories\tnc-sci-ncscobenefits\workspace\data\reforestation_full_griscom_extent_compressed_md5_e42c6c.tif",
        #        'raster2': r"D:\ecoshard\Ecoregions2017_compressed_md5_316061.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
        #    'resample_method': 'near',
        #    'target_raster_path': r"D:\ecoshard\Ecoregions2017_reforest.tif",
        #},
        #{
        #    'expression': 'raster1 + (raster2*220)',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\repositories\tnc-sci-ncscobenefits\workspace\data\forest_conversion_2050_md5_abda51.tif",
        #        'raster2': r"D:\ecoshard\Ecoregions2017_compressed_md5_316061.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
        #    'resample_method': 'near',
        #    'target_raster_path': r"D:\ecoshard\Ecoregions2017_afc.tif",
        #},
        {
            'expression': 'raster1 + (raster2*220)',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\workspace\data\agroforestry_2050_md5_e32698.tif",
                'raster2': r"D:\ecoshard\Ecoregions2017_compressed_md5_316061.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
            'resample_method': 'near',
            'target_raster_path': r"D:\ecoshard\Ecoregions2017_agroforest.tif",
        },
         {
            'expression': 'raster1 + (raster2*220)',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\raster_calculations\align_to_mask_workspace\mangroves_restore_2050_WARPED_near_md5_c8804296c74b6b8dba737e2b25bc0a08.tif",
                'raster2': r"D:\ecoshard\Ecoregions2017_compressed_md5_316061.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
            'resample_method': 'near',
            'target_raster_path': r"D:\ecoshard\Ecoregions2017_mangroverestf.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

#python reclassify_by_table_copied_from_costaricasdr.py  "D:\ecoshard\TNC_NBS\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da.tif" "D:\repositories\tnc-sci-ncscobenefits\esa_nathab_reclass.csv" lucode nathab
#python reclassify_by_table_copied_from_costaricasdr.py  "D:\ecoshard\TNC_NBS\reforestation_full_griscom_extent_compressed_md5_e42c6c.tif" "D:\repositories\tnc-sci-ncscobenefits\esa_nathab_reclass.csv" lucode nathab
#python reclassify_by_table_copied_from_costaricasdr.py  "D:\ecoshard\TNC_NBS\forest_conversion_2050_md5_abda51.tif" "D:\repositories\tnc-sci-ncscobenefits\esa_nathab_reclass.csv" lucode nathab
#python reclassify_by_table_copied_from_costaricasdr.py  "D:\ecoshard\TNC_NBS\agroforestry_2050_md5_e32698.tif" "D:\repositories\tnc-sci-ncscobenefits\esa_nathab_reclass.csv" lucode nathab
#python reclassify_by_table_copied_from_costaricasdr.py  "D:\ecoshard\TNC_NBS\mangroves_restore_2050_WARPED_near_md5_c88042.tif" "D:\repositories\tnc-sci-ncscobenefits\esa_nathab_reclass.csv" lucode nathab

#then align to mask to the global_people_access
# then mask that layer to the nathab

    calculation_list = [
        {
            'expression': '(raster1>1)-(raster2>1)',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\reclassified_marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da_esa_nathab_reclass.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\reclassified_forest_conversion_2050_md5_abda51_esa_nathab_reclass.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\ecoshard\TNC_NBS\DIFF_nathab_afc.tif",
        },
        {
            'expression': '(raster1>1)-(raster2>1)',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\reclassified_reforestation_full_griscom_extent_compressed_md5_e42c6c_esa_nathab_reclass.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\reclassified_marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da_esa_nathab_reclass.tif"
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\ecoshard\TNC_NBS\DIFF_nathab_reforest.tif",
        },
        {
            'expression': '(raster1>1)-(raster2>1)',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\reclassified_mangroves_restore_2050_WARPED_near_md5_c88042_esa_nathab_reclass.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\reclassified_marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da_esa_nathab_reclass.tif"
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\ecoshard\TNC_NBS\DIFF_nathab_mangroverestf.tif",
        },
        {
            'expression': '(raster1>0)-(raster2>0)',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\reclassified_agroforestry_2050_md5_e32698_esa_nathab_reclass.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\reclassified_marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da_esa_nathab_reclass.tif"
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\ecoshard\TNC_NBS\DIFF_nathab_agroforest.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()

    calculation_list = [
        {
            'expression': '(raster2>1)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\raster_calculations\align_to_mask_workspace\ecoshards\global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif",
                'raster2': r"D:\repositories\raster_calculations\align_to_mask_workspace\reclassified_marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_esa_nathab_reclass_WARPED_near_md5_42a4eb50489c1e8945a9d497abf36b18.tif"
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\ecoshard\TNC_NBS\results\nature_access_lspop2019_ESA2020.tif",
        },
        {
            'expression': '(raster2>1)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\raster_calculations\align_to_mask_workspace\ecoshards\global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif",
                'raster2': r"D:\repositories\raster_calculations\align_to_mask_workspace\reclassified_forest_conversion_2050_esa_nathab_reclass_WARPED_near_md5_54fc8ba76f3a7377cbc2e5805f039533.tif"
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\ecoshard\TNC_NBS\results\nature_access_lspop2019_forestconversion050.tif",
        },
        {
            'expression': '(raster2>1)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\raster_calculations\align_to_mask_workspace\ecoshards\global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif",
                'raster2': r"D:\repositories\raster_calculations\align_to_mask_workspace\reclassified_reforestation_full_griscom_extent_compressed_esa_nathab_reclass_WARPED_near_md5_ef2f5737086fa7fb242f4e7676b80ba5.tif"
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\ecoshard\TNC_NBS\results\nature_access_lspop2019_reforestgriscom2050.tif",
        },
        {
            'expression': '(raster2>1)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\raster_calculations\align_to_mask_workspace\ecoshards\global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif",
                'raster2': r"D:\repositories\raster_calculations\align_to_mask_workspace\reclassified_mangroves_restore_2050_WARPED_near_esa_nathab_reclass_WARPED_near_md5_69c87ff3d19c4deb0338e50fe1e23753.tif"
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\ecoshard\TNC_NBS\results\nature_access_lspop2019_mangroverestf2050.tif",
        },
        {
            'expression': '(raster2>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\raster_calculations\align_to_mask_workspace\ecoshards\global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif",
                'raster2': r"D:\repositories\raster_calculations\align_to_mask_workspace\reclassified_marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_esa_nathab_reclass_WARPED_near_md5_42a4eb50489c1e8945a9d497abf36b18.tif"
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\ecoshard\TNC_NBS\results\nature_access_lspop2019_ESA2020ag.tif",
        },
        {
            'expression': '(raster2>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\raster_calculations\align_to_mask_workspace\ecoshards\global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif",
                'raster2': r"D:\repositories\raster_calculations\align_to_mask_workspace\reclassified_agroforestry_2050_esa_nathab_reclass_WARPED_near_md5_e42e580d01a981ebafd00c3c925ac56b.tif"
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\ecoshard\TNC_NBS\results\nature_access_lspop2019_agroforest2050.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


# python nodata_replace.py D:\repositories\tnc-sci-ncscobenefits\scenarios\fertilizers\nci_current_n_app_md5_a7e22.tif D:\repositories\tnc-sci-ncscobenefits\scenarios\fertilizers\extensification_current_practices_n_app_compressed_WARPED_near_md5_eb36647c00aa312194ff6fd5ec0434a9.tif D:\repositories\tnc-sci-ncscobenefits\scenarios\fertilizers\nci_current_n_extens.tif
# python nodata_replace.py D:\repositories\tnc-sci-ncscobenefits\scenarios\fertilizers\nci_current_n_extens.tif D:\repositories\tnc-sci-ncscobenefits\scenarios\fertilizers\ExtensificationNapp_allcrops_rainfedfootprint_gapfilled_observedNappRevB_capped_to_backgroundN_WARPED_near_md5_786db7df0b3e4db9fe17d19fb5a3e78e.tif D:\repositories\tnc-sci-ncscobenefits\scenarios\fertilizers\nci_current_n_extens_background.tif

    calculation_list = [
        {
            'expression': '(raster1>0)*raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\TNC_NBS\DIFF_forest_ReforestGriscom-ESA2020_compressed_md5_4a835e.tif",
                'raster2': r"D:\ecoshard\TNC_NBS\results\NBSdiff_cv_habitat_value_reforest_md5_e17194.tif"
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\workspace\NBSdiff_cv_habitat_value_reforest_f.tif",
        },
        {
            'expression': '(raster1<0)*raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\TNC_NBS\DIFF_forest_Conversion2050-ESA2020_avovr_md5_809e85.tif",
                'raster2': r"D:\ecoshard\TNC_NBS\results\NBSdiff_cv_habitat_value_afc_md5_ab326e.tif"
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\workspace\NBSdiff_cv_habitat_value_afc_f.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

#        calculation_list = [  ###not needed anymore because we fixed ndr to not treat nodata as negative
#        {
#            'expression': '(raster1>0)*raster1',
#            'symbol_to_path_map': {
#                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_n_export_marine_mod_ESA_2020_fertilizer_current_compressed_md5_932883.tif",
#            },
#            'target_nodata': 0,
#            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_n_export_marine_mod_ESA_2020_fertilizer_current_fixed.tif",
#        },
#        {
#            'expression': '(raster1>0)*raster1',
#            'symbol_to_path_map': {
#                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_n_export_tnc_nbs_reforest_fertilizer_current_compressed_md5_f4086f.tif",
#            },
#            'target_nodata': 0,
#            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_n_export_tnc_nbs_reforest_fertilizer_current_fixed.tif",
#        },
#        {
#            'expression': '(raster1>0)*raster1',
#            'symbol_to_path_map': {
#                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_n_export_tnc_nbs_afc_fertilizer_current_compressed_md5_26fee0.tif",
#            },
#            'target_nodata': 0,
#            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_n_export_tnc_nbs_afc_fertilizer_current_fixed.tif",
#        },
#        {
#            'expression': '(raster1>0)*raster1',
#            'symbol_to_path_map': {
#                'raster1': r"D:\repositories\ndr_sdr_global\workspace\global_n_export_tnc_esa2020_md5_9bf10b.tif", # this is a new baseline with the agroforestry parameterization
#            },
#            'target_nodata': 0,
#            'target_raster_path': r"D:\repositories\ndr_sdr_global\workspace\global_n_export_tnc_esa2020agrof_fixed.tif",
#        },
#        {
#            'expression': '(raster1>0)*raster1',
#            'symbol_to_path_map': {
#                'raster1': r"D:\repositories\ndr_sdr_global\workspace\global_n_export_tnc_nbs_agroforest_md5_99a26b.tif",
#            },
#            'target_nodata': 0,
#            'target_raster_path': r"D:\repositories\ndr_sdr_global\workspace\global_n_export_tnc_nbs_agroforest_fixed.tif",
#        },
#        {
#            'expression': '(raster1>0)*raster1',
#            'symbol_to_path_map': {
#                'raster1': r"D:\repositories\ndr_sdr_global\workspace\global_n_export_tnc_nbs_mangroverest_md5_56c7ea.tif",
#            },
#            'target_nodata': 0,
#            'target_raster_path': r"D:\repositories\ndr_sdr_global\workspace\global_n_export_tnc_nbs_mangroverest_fixed.tif",
#        },
#    ]
#
#    for calculation in calculation_list:
#        raster_calculations_core.evaluate_calculation(
#            calculation, TASK_GRAPH, WORKSPACE_DIR)
#
#    TASK_GRAPH.join()
#    TASK_GRAPH.close()
#
#    return

    calculation_list = [
        #{
        #    'expression': 'raster1*raster2',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\Documents\outer_folder_Documents\esa_pixel_area_ha_md5_1dd3298a7c4d25c891a11e01868b5db6.tif",
        #        'raster2': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\mangrove_restore_mask_md5_8bf73b.tif",
        #    },
        #    'target_nodata': 0,
        #    'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
        #    'resample_method': 'near',
        #    'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\mangroves_restore_ha.tif",
        #},
        #{
        #    'expression': 'raster1*raster2',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\Documents\outer_folder_Documents\esa_pixel_area_ha_md5_1dd3298a7c4d25c891a11e01868b5db6.tif",
        #        'raster2': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\silvoculture_extra_mask_md5_d0541c.tif",
        #    },
        #    'target_nodata': 0,
        #    'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
        #    'resample_method': 'near',
        #    'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\silvoculture_extra_ha.tif",
        #},
        #{
        #    'expression': 'raster1*raster2',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\Documents\outer_folder_Documents\esa_pixel_area_ha_md5_1dd3298a7c4d25c891a11e01868b5db6.tif",
        #        'raster2': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\silvoculture_mask_md5_def9ad.tif",
        #    },
        #    'target_nodata': 0,
        #    'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
        #    'resample_method': 'near',
        #    'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\silvoculture_ha.tif",
        #},
        #{
        #    'expression': 'raster1*raster2',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\Documents\outer_folder_Documents\esa_pixel_area_ha_md5_1dd3298a7c4d25c891a11e01868b5db6.tif",
        #        'raster2': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\silvopasture_mask_md5_e5cff9.tif",
        #    },
        #    'target_nodata': 0,
        #    'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
        #    'resample_method': 'near',
        #    'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\silvopasture_ha.tif",
        #},
        {
            'expression': 'raster1*raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\Documents\outer_folder_Documents\esa_pixel_area_ha_md5_1dd3298a7c4d25c891a11e01868b5db6.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\reforestation_mask_md5_0d13c1.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
            'resample_method': 'near',
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\reforestation_ha.tif",
        },
        {
            'expression': 'raster1*raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\Documents\outer_folder_Documents\esa_pixel_area_ha_md5_1dd3298a7c4d25c891a11e01868b5db6.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\forest_conversion_mask.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
            'resample_method': 'near',
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\forest_conversion_ha.tif",
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
            'expression': '((raster1>raster2)+(raster2>raster1))*((raster1*2)-raster2)',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\agroforestry_2050_md5_e32698.tif"
            },
            'target_nodata': 0,
            'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
            'resample_method': 'near',
            'target_datatype': gdal.GDT_Int32,
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\ESAx2-agroforestry.tif",
        },
        {
            'expression': 'raster1-raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\mangroves_restore_2050_md5_eaa31b.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
            'resample_method': 'near',
            'target_datatype': gdal.GDT_Int32,
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\mangroves-ESA.tif",
        },
        {
            'expression': '((raster1>raster2)+(raster2>raster1))*((raster1*2)-raster2)',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\reforestation_full_griscom_extent_compressed_md5_e42c6c.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
            'resample_method': 'near',
            'target_datatype': gdal.GDT_Int32,
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\ESAx2-reforestation.tif",
        },
        {
            'expression': '((raster1>raster2)+(raster2>raster1))*(raster1-(raster2*2))',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\forest_conversion_2050_md5_abda51.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
            'resample_method': 'near',
            'target_datatype': gdal.GDT_Int32,
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\ESA-forestconversionX2.tif",
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
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_sed_export_marine_mod_ESA_2020_compressed_md5_a988c0.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_sed_export_tnc_nbs_reforest_compressed_md5_d2f085.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\NBSdiff_sed_export_reforest.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_sed_export_tnc_nbs_afc_compressed_md5_bf72d5.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_sed_export_marine_mod_ESA_2020_compressed_md5_a988c0.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\NBSdiff_sed_export_afc.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_n_export_marine_mod_ESA_2020_fertilizer_current_compressed_md5_932883.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_n_export_tnc_nbs_reforest_fertilizer_current_compressed_md5_f4086f.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\NBSdiff_n_export_reforest.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_n_export_tnc_nbs_afc_fertilizer_current_compressed_md5_26fee0.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_n_export_marine_mod_ESA_2020_fertilizer_current_compressed_md5_932883.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\NBSdiff_n_export_afc.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    #See "D:\repositories\tnc-sci-ncscobenefits\readme_scenarios.txt" for python commandline commands

    calculation_list = [
        #{
        #    'expression': 'raster1*raster2',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\Reforest_Tree2050_compressed_md5_5972ce.tif",
        #        'raster2': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\Griscom_extent_wgs_compressed_md5_43f6a5.tif"
        #    },
        #    'target_nodata': 255,
        #    'target_datatype': gdal.GDT_Byte,
        #    'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
        #    'resample_method': 'near',
        #    'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\Reforest_Tree2050_Griscom_extent.tif",
        #},
        #{
        #    'expression': '(raster1>0)*(raster1<2)*10 + (raster1>4)*(raster1<6)*190',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\AFC_threats_Curtis_orig_proj_WARPED_near_md5_b56f148c77edb03fa516fe4188440186.tif",
        #    },
        #    'target_nodata': 0,
        #    'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\Curtis_ag10_urban190_mask.tif",
        #},
        #{
        #    'expression': '1-(raster1/100)',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\AFC_Tree2050_compressed_md5_3e88e1.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_datatype': gdal.GDT_Float32,
        #    'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\AFC_1-Tree2050_0-1.tif",
        #},
        #{
        #    'expression': '(raster1<50)*raster1+(raster1>92)*(raster1<160)*raster1+(raster1>170)*raster1',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da.tif",
        #    },
        #    'target_nodata': 0,
        #    'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\marESA2020_forestnodata.tif",
        #},
        #{
        #    'expression': '(raster1>61)*(raster1<63)*(raster2>14)*(raster2<40)+(raster1>71)*(raster1<73)*(raster2>14)*(raster2<40)+(raster1>81)*(raster1<83)*(raster2>14)*(raster2<40)',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da.tif",
        #        'raster2': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\AFC_Tree2050_compressed_md5_3e88e1.tif",
        #    },
        #    'target_nodata': 0,
        #    'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\marESA2020_savanna_back_to_savanna_mask.tif", #because my scenario changed all forest (including classes 62, 72) <40% tree cover and shouldn't have ## but turns out this only accounts for 0.0024% of forest so I'm gonna not worry about it
        #},
        #{
        #    'expression': 'raster1 - raster2',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\repositories\tnc-sci-ncscobenefits\reclassified_forest_conversion_2050_md5_abda51_reclass_forest.tif",
        #        'raster2': r"D:\repositories\tnc-sci-ncscobenefits\reclassified_marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da_reclass_forest.tif",
        #    },
        #    'target_nodata': 0,
        #    'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
        #    'target_datatype': gdal.GDT_Int32,
        #    'resample_method': 'near',
        #    'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\DIFF_forest_Conversion2050-ESA2020.tif",
        #},
        #{
        #    'expression': '(raster1>0)', #this is identical to what we get from the original: python reclassify_by_table_copied_from_costaricasdr.py "D:\repositories\tnc-sci-ncscobenefits\NCS_Refor11_map\NCS_Refor11_map.tif" "D:\repositories\tnc-sci-ncscobenefits\NCS_Refor11_map\NCS_Refor_reclass.csv" Value NCS_Refor
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\repositories\ci-global-restoration\scenarios\potential_forest_regeneration_layers-griscom\sequestration_rate__mean__aboveground__Griscom_restorn_extent__Mg_C_ha_yr.tif",
        #    },
        #    'target_nodata': 0,
        #    'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\Griscom_restorn_extent.tif",
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
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\reclassified_reforestation_full_griscom_extent_reclass_forest.tif", #previously reclassified_reforestation_2050_reclass_forest.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\reclassified_marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da_reclass_forest.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
            'target_datatype': gdal.GDT_Int32,
            'resample_method': 'near',
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\DIFF_forest_ReforestGriscom-ESA2020.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\reclassified_forest_conversion_2050_md5_abda51_reclass_forest.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\reclassified_marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da_reclass_forest.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
            'target_datatype': gdal.GDT_Int32,
            'resample_method': 'near',
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\DIFF_forest_Conversion2050-ESA2020.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\reclassified_marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da_reclass_forest.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\reclassified_marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-1992-v2.0.7cds_wgs84_md5_519311_reclass_forest.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
            #'target_datatype': gdal.GDT_Int32,
            'resample_method': 'near',
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\DIFF_forest_ESA2020-ESA1992.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    #checking conversion of wetlands to forest
    calculation_list = [
        {
            'expression': '(raster1>179)*(raster1<181)+ (raster1>159)*(raster1<161)*3',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\scenarios\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da.tif",
            },
            'target_nodata': 255,
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\wetland1_swamp3_mask_ESA2020.tif",
        },
        {
            'expression': '(raster1>179)*(raster1<181) + (raster1>159)*(raster1<161)*3',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\ci-global-restoration\scenarios\PNV_all_ecosystems\PNV_smith_060420_md5_8dd464e0e23fefaaabe52e44aa296330.tif"
            },
            'target_nodata': 255,
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\wetland1_swamp3_mask_PNV.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\wetland1_swamp3_mask_PNV.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\wetland1_swamp3_mask_ESA2020.tif"
            },
            'target_nodata': 255,
            'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
            'resample_method': 'near',
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\DIFF_PNV-ESA_wetlandswamps.tif",
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
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\total_carbon_spawn_marESA2020.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\total_carbon_spawn_marESA1992.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
            'resample_method': 'near',
            'target_raster_path': r"D:\ecoshard\TNC_NBS\total_carbon_change_2020-1992.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    #python reclassify_by_table_copied_from_costaricasdr.py  D:\repositories\tnc-sci-ncscobenefits\Ecoregions2017_marESA1992_md5_312ef3.tif D:\ecoshard\SpawnESA2010_Carbon_Lookup_Table_md5_f09bca.csv lucode 2010Mean
    #python reclassify_by_table_copied_from_costaricasdr.py  D:\repositories\tnc-sci-ncscobenefits\Ecoregions2017_marESA2020_md5_9e2b63.tif D:\ecoshard\SpawnESA2010_Carbon_Lookup_Table_md5_f09bca.csv lucode 2010Mean
        #renamed to total_carbon_spawn_marESA1992 and total_carbon_spawn_marESA2020

    calculation_list = [
        {
            'expression': 'raster1 + (raster2*220)',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\TNC_NBS\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-1992-v2.0.7cds_wgs84_md5_519311.tif",
                'raster2': r"D:\ecoshard\Ecoregions2017_compressed_md5_316061.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
            'resample_method': 'near',
            'target_raster_path': r"D:\ecoshard\Ecoregions2017_marESA1992.tif",
        },
        {
            'expression': 'raster1 + (raster2*220)',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\TNC_NBS\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da.tif",
                'raster2': r"D:\ecoshard\Ecoregions2017_compressed_md5_316061.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
            'resample_method': 'near',
            'target_raster_path': r"D:\ecoshard\Ecoregions2017_marESA2020.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    #python zonal_stats_by_raster.py "D:\ecoshard\Ecoregions2017_ESA2020modVCFv2_zones_compressed_md5_ab2aa0.tif" "D:\ecoshard\esa_due_globbiomass_2010_md5_3597de.tif" --basename globbiomass_2010
    # python zonal_stats_by_raster.py "D:\ecoshard\Ecoregions2017_ESA2020modVCFv2_zones_compressed_md5_ab2aa0.tif" "D:\ecoshard\aboveground_biomass_carbon_2010_md5_4be351.tif" --basename spawn_aboveground
    # python zonal_stats_by_raster.py "D:\ecoshard\Ecoregions2017_ESA2020modVCFv2_zones_compressed_md5_ab2aa0.tif" "D:\ecoshard\belowground_biomass_carbon_2010_md5_d68b5d.tif" --basename spawn_belowground
    # python zonal_stats_by_raster.py "D:\ecoshard\Lesiv_FML_ESA2020modVCFv2_zones.tif" "D:\ecoshard\Manageable_Carbon_2018\Total_Carbon_2010_compressed_md5_62290b.tif" --basename total_carbon_FMLzones
    # python zonal_stats_by_raster.py "D:\ecoshard\Ecoregions2017_Lesiv_FML_zones_compressed_md5_795ab7.tif" "D:\ecoshard\Manageable_Carbon_2018\Total_Carbon_2010_compressed_md5_62290b.tif" --basename TotalC_FMLzones
    # python zonal_stats_by_raster.py "D:\ecoshard\Ecoregions2017_Lesiv_FML_zones_compressed_md5_795ab7.tif" "D:\ecoshard\aboveground_biomass_carbon_2010_md5_4be351.tif" --basename spawn_aboveground_FMLzones
    # python zonal_stats_by_raster.py "D:\ecoshard\Ecoregions2017_ESA2010smooth_zones_md5_d9fe48.tif" "D:\ecoshard\aboveground_biomass_carbon_2010_md5_4be351.tif" --basename spawn_aboveground_ESA2010zones
    # python zonal_stats_by_raster.py "D:\ecoshard\Ecoregions2017_Lesiv_FML_zones_compressed_md5_795ab7.tif" "D:\ecoshard\belowground_biomass_carbon_2010_md5_d68b5d.tif" --basename spawn_belowground_FMLzones
    # python zonal_stats_by_raster.py "D:\ecoshard\Ecoregions2017_ESA2010smooth_zones_md5_d9fe48.tif" "D:\ecoshard\total_biomass_carbon_2010_spawn_rescaled.tif" --basename spawn_total_rescaled_ESA2010zones

    #scenarios!
    # python create_scenario.py "D:\ecoshard\TNC_NBS\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da.tif" "D:\ecoshard\TNC_NBS\sequestration_rate__mean__aboveground__Griscom_restorn_extent__Mg_C_ha_yr.tif" 0.1 --flip_target_path "D:\ecoshard\CI_PPC\scenarios\Sc3v1_PNVnoag_md5_c07865b995f9ab2236b8df0378f9206f.tif"
    # python create_scenario.py "D:\ecoshard\TNC_NBS\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da.tif" "D:\ecoshard\TNC_NBS\sequestration_rate__mean__aboveground__Griscom_restorn_extent__Mg_C_ha_yr.tif" 0.1 --flip_target_val 999
    # python create_scenario.py "D:\ecoshard\TNC_NBS\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da.tif" "D:\ecoshard\TNC_NBS\AFCv1_reversed_conv_rate_global_md5_e62e00.tif" 0.00001 --flip_target_val 30

    calculation_list = [
        {
            'expression': 'raster1 + (raster2*220)', #Rich says I should actually have multiplied by 221 (max(raster1)+1) but this seems to work
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2010-v2.0.7_smooth_compressed.tif",
                'raster2': r"D:\ecoshard\Ecoregions2017_compressed_md5_316061.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
            'resample_method': 'near',
            'target_raster_path': r"D:\ecoshard\Ecoregions2017_ESA2010smooth_zones.tif",
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
            'expression': 'raster1 + (raster2*53)',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\Lesiv_FML_ndv0_compressed_md5_a3fe94.tif",
                'raster2': r"D:\ecoshard\Ecoregions2017_compressed_md5_316061.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
            'resample_method': 'near',
            'target_raster_path': r"D:\ecoshard\Ecoregions2017_Lesiv_FML_zones.tif",
            #'target_datatype': gdal.GDT_Int32,
            #'target_datatype': gdal.GDT_Float32,
            #'target_datatype': gdal.GDT_Byte,
            #'target_datatype': gdal.GDT_Int16,
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
            'expression': '(raster1<0)*0 + (raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\Lesiv_FML_v3-2_compressed_md5_3fe35d.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\ecoshard\Lesiv_FML_ndv0.tif"
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
                'raster1': r"D:\ecoshard\AFCv1_conv_rate_global_md5_7ac543.tif",
            },
            'target_nodata': -9999,
            'default_nan': -9999,
            'target_raster_path': "AFCv1_reversed_conv_rate_global.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    # first make sure to ecoshard --ndv to set ndv to something other than 0 (like -1). Then:
    # python nodata_replace.py "D:\repositories\cnc_global_cv\global_cv_workspace\ndv_-1.0_marESA1992_cv_habitat_value_md5_a59070.tif" "D:\repositories\raster_calculations\landmask0_10s_md5_54231a.tif" cv_habitat_value_marESA1992_full_land.tif
    # python nodata_replace.py "D:\repositories\cnc_global_cv\global_cv_workspace\ndv_-1.0_marESA2020_cv_habitat_value_md5_520390.tif" "D:\repositories\raster_calculations\landmask0_10s_md5_54231a.tif" cv_habitat_value_marESA2020_full_land.tif


    calculation_list = [
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\cnc_global_cv\global_cv_workspace\cv_habitat_value_marESA2020_full_land_md5_bac749.tif",
                'raster2': r"D:\repositories\cnc_global_cv\global_cv_workspace\cv_habitat_value_marESA1992_full_land_md5_454eb4.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "cv_habitat_value_marESA2020-1992_change.tif",
        },
        #{
        #    'expression': 'raster1 - raster2',
        #    'symbol_to_path_map': {
        #        'raster1': "nature_access_lspop2019_marESA2020.tif",
        #        'raster2': "nature_access_lspop2019_marESA1992.tif",
        #    },
        #    'target_nodata': -1,
        #    'target_raster_path': "nature_access_lspop2019_marESA2020-1992_change.tif",
        #},
        #{
        #    'expression': '(raster1>0)*raster2',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\repositories\raster_calculations\align_to_mask_workspace\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-1992-v2.0.7cds_compressed_hab_mask_WARPED_near_md5_bb7337555b6a0f384cb77d9e68cfdb55.tif",
        #        'raster2': r"D:\repositories\raster_calculations\align_to_mask_workspace\ecoshards\global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif",
        #    },
        #    'target_nodata': -1,
        #    'target_raster_path': "nature_access_lspop2019_marESA1992.tif",
        #},
        #{
        #    'expression': '(raster1>0)*raster2',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\repositories\raster_calculations\align_to_mask_workspace\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_hab_mask_WARPED_near_md5_66e6ab25e34464d153d788ed0182832e.tif",
        #        'raster2': r"D:\repositories\raster_calculations\align_to_mask_workspace\ecoshards\global_people_access_population_2019_60.0m_md5_d264d371bd0d0a750b002a673abbb383.tif",
        #    },
        #    'target_nodata': -1,
        #    'target_raster_path': "nature_access_lspop2019_marESA2020.tif",
        #},
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [ #see README_NDR_RESULTS.txt
        {
            'expression': '(raster1>0)*raster1 + (raster1<0)*-9999',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_n_retention_marine_mod_ESA_2020_fertilizer_current_compressed_md5_1d46ad.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "global_n_retention_ESAmar_2020_fertilizer_current_valid.tif",
        },
        {
            'expression': '(raster1>0)*raster1 + (raster1<0)*-9999',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_n_retention_marine_mod_ESA_1992_fertilizer_current_compressed_md5_834235.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "global_n_retention_ESAmar_1992_fertilizer_current_valid.tif",
        },
        {
            'expression': '(raster1>0)*raster1 + (raster1<0)*-9999',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_n_export_marine_mod_ESA_2020_fertilizer_current_compressed_md5_8ba187.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "global_n_export_ESAmar_2020_fertilizer_current_valid.tif",
        },
        {
            'expression': '(raster1>0)*raster1 + (raster1<0)*-9999',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_n_export_marine_mod_ESA_1992_fertilizer_current_compressed_md5_b1bb72.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "global_n_export_ESAmar_1992_fertilizer_current_valid.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    # Can't just use raster calculator on these ones because they are nodata in different places (because of expansion/contraction of cropland)
    # "realized_pollination_on_hab_marESA_2020-1992_change.tif":
    # python add_sub_missing_as_0.py "D:\repositories\pollination_sufficiency\workspace_realized_pollination\ppl_fed_per_pixel__ESA2020mar_mask_to_hab__ESA2020mar_compressed.tif" "D:\repositories\pollination_sufficiency\workspace_realized_pollination\ppl_fed_per_pixel__ESA1992mar_mask_to_hab__ESA1992mar_compressed.tif"
    # "realized_pollination_on_ag_marESA_2020-1992_change.tif"
    # python add_sub_missing_as_0.py "D:\repositories\pollination_sufficiency\pollination_ppl_fed_on_ag_10s_esa2020mar_md5_684b65.tif" "D:\repositories\pollination_sufficiency\pollination_ppl_fed_on_ag_10s_esa1992mar_md5_073a8b.tif"
    ## But this (above) doesn't work right now either. So have to do it the long way, nodata_replace, and then raster calc
    # python nodata_replace.py "D:\results\ppl_fed_per_pixel__ESA2020mar_mask_to_hab__ESA2020mar_compressed.tif" "D:\repositories\raster_calculations\landmask0_10s_md5_54231a.tif" realized_polllination_on_hab_ESA2020mar.tif
    # python nodata_replace.py "D:\results\ppl_fed_per_pixel__ESA1992mar_mask_to_hab__ESA1992mar_compressed.tif" "D:\repositories\raster_calculations\landmask0_10s_md5_54231a.tif" realized_polllination_on_hab_ES1992mar.tif
    # python nodata_replace.py "D:\results\pollination_ppl_fed_on_ag_10s_esa2020mar_md5_684b65.tif" "D:\repositories\raster_calculations\landmask0_10s_md5_54231a.tif" realized_polllination_on_ag_ESA2020mar.tif
    # python nodata_replace.py "D:\results\pollination_ppl_fed_on_ag_10s_esa1992mar_md5_073a8b.tif" "D:\repositories\raster_calculations\landmask0_10s_md5_54231a.tif" realized_polllination_on_ag_ESA1992mar.tif


    calculation_list = [
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\results\realized_polllination_on_hab_ESA2020mar_md5_5acc75.tif",
                'raster2': r"D:\results\realized_polllination_on_hab_ES1992mar_md5_d7ef26.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "realized_pollination_on_hab_marESA_2020-1992_fullchange.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\results\realized_polllination_on_ag_ESA2020mar_md5_da610a.tif",
                'raster2': r"D:\results\realized_polllination_on_ag_ESA1992mar_md5_d3d0d3.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_pollination_on_ag_marESA_2020-1992_fullchange.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\raster_calculations\global_n_export_ESAmar_2020_fertilizer_current_valid_md5_e2b294.tif",
                'raster2': r"D:\repositories\raster_calculations\global_n_export_ESAmar_1992_fertilizer_current_valid_md5_f4b421.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "n_export_marineESA_2020-1992_change_val.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_sed_export_marine_mod_ESA_2020_compressed_md5_a988c0.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_sed_export_marine_mod_ESA_1992_compressed_md5_18eaae.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "sed_export_marineESA_2020-1992_change.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\raster_calculations\global_n_retention_ESAmar_2020_fertilizer_current_valid_md5_82fc1e.tif",
                'raster2': r"D:\repositories\raster_calculations\global_n_retention_ESAmar_1992_fertilizer_current_valid_md5_86031b.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "n_retention_marineESA_2020-1992_change_val.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_sed_deposition_marine_mod_ESA_2020_compressed_md5_1785d5.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\workspace\global_sed_deposition_marine_mod_ESA_1992_compressed_md5_38799c.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "sed_deposition_marineESA_2020-1992_change.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\pollination_sufficiency\workspace_poll_suff\churn\hab_mask\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da_hab_mask.tif",
                'raster2': r"D:\repositories\pollination_sufficiency\workspace_poll_suff\churn\hab_mask\marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-1992-v2.0.7cds_compressed_md5_83ec1b_hab_mask.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "habmask_marESA_2020-1992_change.tif",
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
