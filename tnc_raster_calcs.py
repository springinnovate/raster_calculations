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

    calculation_list = [
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\tnc-sci-ncscobenefits\reclassified_reforestation_intermediate_reclass_forest.tif",
                'raster2': r"D:\repositories\tnc-sci-ncscobenefits\reclassified_marine_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_e6a8da_reclass_forest.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
            'target_datatype': gdal.GDT_Int32,
            'resample_method': 'near',
            'target_raster_path': r"D:\repositories\tnc-sci-ncscobenefits\forest_ESA2020-intermediate",
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
