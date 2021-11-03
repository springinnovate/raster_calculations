"""These calculations are for the Critical Natural Capital paper."""
#cd C:\Users\Becky\Documents\raster_calculations
#conda activate py38_gdal312

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
import taskgraph
#import pygeoprocessing
import ecoshard.geoprocessing as pygeoprocessing

gdal.SetCacheMax(2**30)

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

    # CNC calculations
    
    #to find nodata value: 
    #gdalinfo [raster path]

    calculation_list = [
        {
            'expression': '(raster1>0)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\S_90_md5_5d18924c69519ec76993f4d58a7b2687.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "coastal_overlapping_A90.tif",
        },
        {
            'expression': '(raster1>0)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\T_90_md5_6a0142de25bb3b5a107f7abae694c5b0.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "timber_overlapping_A90.tif",
        },
        {
            'expression': '(raster1>0)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\U_90_md5_258160b638e742e91b84979e6b2c748f.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "flood_overlapping_A90.tif",
        },
        {
            'expression': '(raster1>0)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\V_90_md5_eeb6b515ad2f25a3ad76099e07e030bc.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "fuelwood_overlapping_A90.tif",
        },
        {
            'expression': '(raster1>0)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\W_90_md5_de1e7dc33c7227cdbcda5b7e6f9919bb.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "fwfish_overlapping_A90.tif",
        },
        {
            'expression': '(raster1>0)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\X_90_md5_0cc1f3aeb8e1a566a6b220bf9986b828.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "grazing_overlapping_A90.tif",
        },
        {
            'expression': '(raster1>0)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\Y_90_md5_f8393b73f3548658f610ac47acea72e7.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "marinefish_overlapping_A90.tif",
        },
        {
            'expression': '(raster1>0)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\Z_90_md5_1b9d0deb1e16f6975dc3402aacf4846e.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "natureaccess_overlapping_A90.tif",
        },
        {
            'expression': '(raster1>0)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A1_90_md5_1fb33de8a6ced1d1f54dcc7debed3c6c.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "nitrogen_overlapping_A90.tif",
        },
        {
            'expression': '(raster1>0)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\B1_90_md5_14484122eba5a970559c57a48621d3fd.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "pollination_overlapping_A90.tif",
        },
        {
            'expression': '(raster1>0)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\C1_90_md5_3246d7fc06267a18f59ca9a8decf64fe.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "reeftourism_overlapping_A90.tif",
        },
        {
            'expression': '(raster1>0)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\D1_90_md5_ee81ad59355f2309c2ecb882e788454a.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "sediment_overlapping_A90.tif",
        },
        {
            'expression': '(raster1>0)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\S_90_md5_5d18924c69519ec76993f4d58a7b2687.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "coastal_overlapping_A90.tif",
        },
        {
            'expression': '(raster1>0)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\H1_90_md5_7973783ac2786f9d521a4b8b4cf5d68d.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "carbon_overlapping_A90.tif",
        },
        {
            'expression': '(raster1>0)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\I1_90_md5_54ad2f227abc1cf66ed23cc6d3b72d47.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "moisture_overlapping_A90.tif",
        },       
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [ 
    #    {
    #        'expression': 'raster1*(raster2<2)*(raster2)',
    #        'symbol_to_path_map': {            
    #            'raster1': r"C:\Users\Becky\Documents\raster_calculations\lspop2017_compressed_md5_53e326f463a2c8a8fa92d8dea6f37df1.tif",
    #            'raster2': r"C:\Users\Becky\Documents\cnc_project\overlap\beneficiaries\downstream_mask_A_90_WARPED_near_md5_0ed997ee57533433c6372e070592e880_compressed.tif",
    #        },
    #        'target_nodata': 0,
    #        'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
    #        'resample_method': 'average',
    #        'target_raster_path': "lspop_on_downstream_of_10sA90.tif",
    #    },
    #    {
    #        'expression': 'raster1*(raster2<2)*(raster2)',
    #        'symbol_to_path_map': {            
    #            'raster1': r"C:\Users\Becky\Documents\raster_calculations\lspop2017_compressed_md5_53e326f463a2c8a8fa92d8dea6f37df1.tif",
    #            'raster2': r"C:\Users\Becky\Documents\cnc_project\overlap\beneficiaries\downstream_mask_C_90_WARPED_near_md5_6a33ab63b7ac8fb9a679e192741bcac5_compressed.tif",
    #        },
    #        'target_nodata': 0,
    #        'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
    #        'resample_method': 'average',
    #        'target_raster_path': "lspop_on_downstream_of_10sC90.tif",
    #    },    
    #    {
    #        'expression': 'raster1*(raster2<2)*(raster2)',
    #        'symbol_to_path_map': {            
    #            'raster1': r"C:\Users\Becky\Documents\raster_calculations\lspop2017_compressed_md5_53e326f463a2c8a8fa92d8dea6f37df1.tif",
    #            'raster2': r"C:\Users\Becky\Documents\cnc_project\overlap\beneficiaries\downstream_mask_masked_all_nathab_wstreams_esa2015_md5_c291ff6ef7db1d5ff4d95a82e0f035de.tif",
    #        },
    #        'target_nodata': 0,
    #        'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
    #        'resample_method': 'average',
    #        'target_raster_path': "lspop_on_downstream_of_nathab.tif",
    #    },
    #    {
    #        'expression': 'raster1*(raster2<2)*(raster2)',
    #        'symbol_to_path_map': {            
    #            'raster1': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\floodplains\floodplains_masked_pop_30s_md5_c027686bb9a9a36bdababbe8af35d696.tif",
    #            'raster2': r"C:\Users\Becky\Documents\cnc_project\overlap\beneficiaries\downstream_mask_A_90_WARPED_near_md5_0ed997ee57533433c6372e070592e880_compressed.tif",
    #        },
    #        'target_nodata': 0,
    #        'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
    #        'resample_method': 'average',
    #        'target_raster_path': "lspopfloodplains_on_downstream_of_10sA90.tif",
    #    },
        {
            'expression': 'raster1*(raster2<2)*(raster2)',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\floodplains\floodplains_masked_pop_30s_md5_c027686bb9a9a36bdababbe8af35d696.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\overlap\beneficiaries\downstream_mask_masked_all_nathab_wstreams_esa2015_md5_c291ff6ef7db1d5ff4d95a82e0f035de.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
            'resample_method': 'average',
            'target_raster_path': "lspopfloodplains_on_downstream_of_nathab.tif",
        },
    #    {
    #        'expression': 'raster1*(raster2>0)',
    #        'symbol_to_path_map': {            
    #            'raster1': r"C:\Users\Becky\Documents\cnc_project\original_rasters\cnc_cv\normalized_pop_on_hab\norm_total_pop_hab_mask_coverage_md5_8f31e5fc65bf07488b4945b35f493d3f.tif",
    #            'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\wgs\A_90_WARPED_near_md5_0ed997ee57533433c6372e070592e880.tif",
    #        },
    #        'target_nodata': 0,
    #        'target_pixel_size': (0.002777777777999999864,-0.002777777777999999864),
    #        'resample_method': 'average',
    #        'target_raster_path': "coastal_pop-on-hab_A90.tif",
    #    },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [ 
        {
            'expression': 'raster1*(raster2<2)*(raster2)',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\raster_calculations\lspop2017_compressed_md5_53e326f463a2c8a8fa92d8dea6f37df1.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\overlap\beneficiaries\global_mask_access_A_90_60.0m_WARPED_wgs_near_md5_b8de1aaeec4a800b7626944dd6df52ba.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
            'target_raster_path': "lspop_within_60min_of_10sA90.tif",
        },
        {
            'expression': 'raster1*(raster2<2)*(raster2)',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\raster_calculations\lspop2017_compressed_md5_53e326f463a2c8a8fa92d8dea6f37df1.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\overlap\beneficiaries\global_mask_access_C_90_60.0m_WARPED_wgs_near_md5_71f1dc947f32915ab153873c64fa3827.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
            'target_raster_path': "lspop_within_60min_of_10sC90.tif",
        },    
        {
            'expression': 'raster1*(raster2<2)*(raster2)',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\raster_calculations\lspop2017_compressed_md5_53e326f463a2c8a8fa92d8dea6f37df1.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\overlap\beneficiaries\global_mask_access_masked_all_nathab_wstreams_esa2015_nodata_60.0m_WARPED_wgs_near_md5_21e1df6d6d886b6f388948d7fc660e77.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
            'target_raster_path': "lspop_within_60min_of_nathab.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    #did average_rasters.py on downstream and within 60 minutes and then did a nodata_replace with the coastal pop:
    #  python nodata_replace.py "C:\Users\Becky\Documents\cnc_project\overlap\beneficiaries\lspop_downstream_within_60min_A90_average_raster.tif" "C:\Users\Becky\Documents\raster_calculations\align_to_mask_workspace\coastalpop_on_A90_WARPED_near_md5_131687f40a4985e81e23331f6d479105.tif" "lspop_downstream_within60min_coastal.tif"
    # BUT THIS IS ACTUALLY NO GOOD BECAUSE IT'S THE COASTAL POP BENEFITTING FROM A90 MAPPED TO A90, NOT THE PEOPLE WITHIN PROTECTIVE DISTANCE OF A90 HABITAT!!!

   

    calculation_list = [ 
        {
            'expression': '(raster1>11)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\overlap\drop1\sum_drop1_compressed_md5_c67b069d9e812dad11f67daf6cd04435.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "pixels_always_selected.tif",
        },
        {
            'expression': '(raster1>=11)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\overlap\drop1\sum_drop1_compressed_md5_c67b069d9e812dad11f67daf6cd04435.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "pixels_90pct_selected.tif",
        },
        {
            'expression': '(raster1>=6)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\overlap\drop1\sum_drop1_compressed_md5_c67b069d9e812dad11f67daf6cd04435.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "pixels_50pct_selected.tif",
        },                
    ]
    
    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return



    

    
    #Fig 1: 
    #First - Run cna_masks.py with the following inputs using (near) for reproject to WGS & mask to land and eez
    #MASK_ECOSHARD_URL = (
    #'https://storage.googleapis.com/critical-natural-capital-ecoshards/habmasks/landmask_10s_md5_748981cbf6ebf22643a3a3e655ec50ce_compressed_reduce8x.tif')
    #'https://storage.googleapis.com/critical-natural-capital-ecoshards/habmasks/EEZ_mask_0027_compressed_md5_0f25e6a690fef616d34c5675b57e76f8_reduce8x.tif')
    # RASTER_LIST = [
    #('solution_A_all_targets_2km_compressed_md5_46647c1d514427417a588674a98fd93b.tif', True, False), 
    #('solution_B_all_targets_2km_compressed_md5_46640e0340231bc3f7a3d9c286985d3f.tif', True, False),
    #Then do: python nodata_replace.py "C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\wgs\solution_A_all_targets_2km_compressed_WARPED_near_MASKED_land_md5_d95883e02b205e300b232ef156bcc45b.tif" "C:\Users\Becky\Documents\cnc_project\supporting_layers\natural_assets_wstreams_.0027_to_.022_0s_md5_48c16399d89fe8f9411c4e905873b40f.tif" solution_A_all_targets_2km_land_wgs.02_fill.tif
    #python nodata_replace.py "C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\wgs\solution_A_all_targets_2km_compressed_WARPED_near_MASKED_eez_md5_227e3df7cb09bfb6c7f183f8dc721157.tif" "C:\Users\Becky\Documents\cnc_project\supporting_layers\EEZ_mask_0027_compressed_md5_0f25e6a690fef616d34c5675b57e76f8_reduce8x.tif" solution_A_all_targets_2km_eez_wgs.02_fill.tif
    #python nodata_replace.py "C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\wgs\solution_B_all_targets_2km_compressed_WARPED_near_MASKED_land_md5_9e08e0b58df950a4e9772c0a8e36e867.tif" "C:\Users\Becky\Documents\cnc_project\supporting_layers\natural_assets_wstreams_.0027_to_.022_0s_md5_48c16399d89fe8f9411c4e905873b40f.tif" solution_B_all_targets_2km_land_wgs.02_fill.tif
    #python nodata_replace.py "C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\wgs\solution_B_all_targets_2km_compressed_WARPED_near_MASKED_eez_md5_ef870a38c66a26c1b718a8ffde07c4fa.tif" "C:\Users\Becky\Documents\cnc_project\supporting_layers\EEZ_mask_0027_compressed_md5_0f25e6a690fef616d34c5675b57e76f8_reduce8x.tif" solution_B_all_targets_2km_eez_wgs.02_fill.tif
    #python nodata_replace.py "C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\wgs\solution_C_all_targets_2km_compressed_WARPED_near_md5_c2733d7dc996e039f2ffdcf4a1ce412b.tif" "C:\Users\Becky\Documents\cnc_project\supporting_layers\natural_assets_wstreams_.0027_to_.022_0s_md5_48c16399d89fe8f9411c4e905873b40f.tif" solution_C_all_targets_2km_land_wgs.02_fill.tif

    
    #Overlap for Fig 2

    single_expression = {
        'expression': '(raster1>0)*(raster2<1)*(raster3<1) + 2*(raster1<1)*(raster2>0)*(raster3<1) + 3*(raster3>0)',
        'symbol_to_path_map': {            
            'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\C_90_md5_bdf604015a7b1c7c78845ad716d568ef.tif",
            'raster3': 'A90_C90_overlap.tif',
        },
        'target_nodata': -1,
        'target_raster_path': "ncp1_climate2_overlap3.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': '(raster1>0)*(raster2<1)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\C_90_md5_bdf604015a7b1c7c78845ad716d568ef.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "A90_nonoverlapping_C90.tif",
        }, 
        {
            'expression': '(raster1<1)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\C_90_md5_bdf604015a7b1c7c78845ad716d568ef.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "C90_nonoverlapping_A90.tif",
        }, 
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\C_90_md5_bdf604015a7b1c7c78845ad716d568ef.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "A90_C90_overlap.tif",
        }, 
        {
            'expression': '(raster1>0)*(raster2<1)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\B_90_md5_b08de6ccc0fc3e122450c1ccfcb8b60d.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\D_90_md5_893abc862f38d66e222a99fa1808dd34.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "B90_nonoverlapping_D90.tif",
        }, 
        {
            'expression': '(raster1>0)*(raster2<1)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\C_90_md5_bdf604015a7b1c7c78845ad716d568ef.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\D_90_md5_893abc862f38d66e222a99fa1808dd34.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "C90_nonoverlapping_D90.tif",
        },       
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    #Overlap analyses to make correlation table 
    #coastal - S_90_md5_5d18924c69519ec76993f4d58a7b2687.tif 
    #timber - T_90_md5_6a0142de25bb3b5a107f7abae694c5b0.tif
    #flood - U_90_md5_258160b638e742e91b84979e6b2c748f.tif
    #fuelwood - V_90_md5_eeb6b515ad2f25a3ad76099e07e030bc.tif
    #fwfish - W_90_md5_de1e7dc33c7227cdbcda5b7e6f9919bb.tif
    #grazing - X_90_md5_0cc1f3aeb8e1a566a6b220bf9986b828.tif
    #marinefish - Y_90_md5_f8393b73f3548658f610ac47acea72e7.tif  
    #natureacces - Z_90_md5_1b9d0deb1e16f6975dc3402aacf4846e.tif
    #nitrogen - A1_90_md5_1fb33de8a6ced1d1f54dcc7debed3c6c.tif
    #pollination - B1_90_md5_14484122eba5a970559c57a48621d3fd.tif
    #reeftourism - C1_90_md5_3246d7fc06267a18f59ca9a8decf64fe.tif
    #sediment - D1_90_md5_ee81ad59355f2309c2ecb882e788454a.tif
    #carbon - H1_90_md5_7973783ac2786f9d521a4b8b4cf5d68d
    #moisture - I1_90_md5_54ad2f227abc1cf66ed23cc6d3b72d47

#SINGLESERVICE_NONOVERLAPS
    
    calculation_list = [
        {
            'expression': '(raster1<1)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\S_90_md5_5d18924c69519ec76993f4d58a7b2687.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "coastal_nonoverlapping_A90.tif",
        },
        {
            'expression': '(raster1<1)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\T_90_md5_6a0142de25bb3b5a107f7abae694c5b0.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "timber_nonoverlapping_A90.tif",
        },
        {
            'expression': '(raster1<1)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\U_90_md5_258160b638e742e91b84979e6b2c748f.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "flood_nonoverlapping_A90.tif",
        },
        {
            'expression': '(raster1<1)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\V_90_md5_eeb6b515ad2f25a3ad76099e07e030bc.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "fuelwood_nonoverlapping_A90.tif",
        },
        {
            'expression': '(raster1<1)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\W_90_md5_de1e7dc33c7227cdbcda5b7e6f9919bb.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "fwfish_nonoverlapping_A90.tif",
        },
        {
            'expression': '(raster1<1)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\X_90_md5_0cc1f3aeb8e1a566a6b220bf9986b828.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "grazing_nonoverlapping_A90.tif",
        },
        {
            'expression': '(raster1<1)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\Y_90_md5_f8393b73f3548658f610ac47acea72e7.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "marinefish_nonoverlapping_A90.tif",
        },
        {
            'expression': '(raster1<1)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\Z_90_md5_1b9d0deb1e16f6975dc3402aacf4846e.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "natureaccess_nonoverlapping_A90.tif",
        },
        {
            'expression': '(raster1<1)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A1_90_md5_1fb33de8a6ced1d1f54dcc7debed3c6c.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "nitrogen_nonoverlapping_A90.tif",
        },
        {
            'expression': '(raster1<1)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\B1_90_md5_14484122eba5a970559c57a48621d3fd.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "pollination_nonoverlapping_A90.tif",
        },
        {
            'expression': '(raster1<1)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\C1_90_md5_3246d7fc06267a18f59ca9a8decf64fe.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "reeftourism_nonoverlapping_A90.tif",
        },
        {
            'expression': '(raster1<1)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\D1_90_md5_ee81ad59355f2309c2ecb882e788454a.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "sediment_nonoverlapping_A90.tif",
        },
        {
            'expression': '(raster1<1)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\S_90_md5_5d18924c69519ec76993f4d58a7b2687.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "coastal_nonoverlapping_A90.tif",
        },
        {
            'expression': '(raster1<1)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\H1_90_md5_7973783ac2786f9d521a4b8b4cf5d68d.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "carbon_nonoverlapping_A90.tif",
        },
        {
            'expression': '(raster1<1)*(raster2>0)', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\I1_90_md5_54ad2f227abc1cf66ed23cc6d3b72d47.tif",
             },
            'target_nodata': -1,
            'target_raster_path': "moisture_nonoverlapping_A90.tif",
        },       
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    
#PAIRWISE OVERLAPS
    calculation_list = [
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\H1_90_md5_7973783ac2786f9d521a4b8b4cf5d68d.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\S_90_md5_5d18924c69519ec76993f4d58a7b2687.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_carbon_coastal_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\I1_90_md5_54ad2f227abc1cf66ed23cc6d3b72d47.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\S_90_md5_5d18924c69519ec76993f4d58a7b2687.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_moisture_coastal_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\H1_90_md5_7973783ac2786f9d521a4b8b4cf5d68d.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\T_90_md5_6a0142de25bb3b5a107f7abae694c5b0.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_carbon_timber_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\H1_90_md5_7973783ac2786f9d521a4b8b4cf5d68d.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\U_90_md5_258160b638e742e91b84979e6b2c748f.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_carbon_flood_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\H1_90_md5_7973783ac2786f9d521a4b8b4cf5d68d.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\V_90_md5_eeb6b515ad2f25a3ad76099e07e030bc.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_carbon_fuelwood_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\H1_90_md5_7973783ac2786f9d521a4b8b4cf5d68d.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\W_90_md5_de1e7dc33c7227cdbcda5b7e6f9919bb.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_carbon_fwfish_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\H1_90_md5_7973783ac2786f9d521a4b8b4cf5d68d.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\X_90_md5_0cc1f3aeb8e1a566a6b220bf9986b828.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_carbon_grazing_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\H1_90_md5_7973783ac2786f9d521a4b8b4cf5d68d.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\Z_90_md5_1b9d0deb1e16f6975dc3402aacf4846e.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_carbon_natureaccess_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\H1_90_md5_7973783ac2786f9d521a4b8b4cf5d68d.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A1_90_md5_1fb33de8a6ced1d1f54dcc7debed3c6c.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_carbon_nitrogen_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\H1_90_md5_7973783ac2786f9d521a4b8b4cf5d68d.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\B1_90_md5_14484122eba5a970559c57a48621d3fd.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_carbon_pollination_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\H1_90_md5_7973783ac2786f9d521a4b8b4cf5d68d.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\D1_90_md5_ee81ad59355f2309c2ecb882e788454a.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_carbon_sediment_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\H1_90_md5_7973783ac2786f9d521a4b8b4cf5d68d.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\C1_90_md5_3246d7fc06267a18f59ca9a8decf64fe.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_carbon_reeftourism_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\H1_90_md5_7973783ac2786f9d521a4b8b4cf5d68d.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\I1_90_md5_54ad2f227abc1cf66ed23cc6d3b72d47.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_carbon_moisture_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\I1_90_md5_54ad2f227abc1cf66ed23cc6d3b72d47.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\T_90_md5_6a0142de25bb3b5a107f7abae694c5b0.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_moisture_timber_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\I1_90_md5_54ad2f227abc1cf66ed23cc6d3b72d47.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\U_90_md5_258160b638e742e91b84979e6b2c748f.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_moisture_flood_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\I1_90_md5_54ad2f227abc1cf66ed23cc6d3b72d47.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\V_90_md5_eeb6b515ad2f25a3ad76099e07e030bc.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_moisture_fuelwood_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\I1_90_md5_54ad2f227abc1cf66ed23cc6d3b72d47.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\W_90_md5_de1e7dc33c7227cdbcda5b7e6f9919bb.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_moisture_fwfish_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\I1_90_md5_54ad2f227abc1cf66ed23cc6d3b72d47.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\X_90_md5_0cc1f3aeb8e1a566a6b220bf9986b828.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_moisture_grazing_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\I1_90_md5_54ad2f227abc1cf66ed23cc6d3b72d47.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\Z_90_md5_1b9d0deb1e16f6975dc3402aacf4846e.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_moisture_natureaccess_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\I1_90_md5_54ad2f227abc1cf66ed23cc6d3b72d47.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A1_90_md5_1fb33de8a6ced1d1f54dcc7debed3c6c.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_moisture_nitrogen_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\I1_90_md5_54ad2f227abc1cf66ed23cc6d3b72d47.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\B1_90_md5_14484122eba5a970559c57a48621d3fd.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_moisture_pollination_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\I1_90_md5_54ad2f227abc1cf66ed23cc6d3b72d47.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\D1_90_md5_ee81ad59355f2309c2ecb882e788454a.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_moisture_sediment_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\S_90_md5_5d18924c69519ec76993f4d58a7b2687.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\T_90_md5_6a0142de25bb3b5a107f7abae694c5b0.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_coastal_timber_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\S_90_md5_5d18924c69519ec76993f4d58a7b2687.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\U_90_md5_258160b638e742e91b84979e6b2c748f.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_coastal_flood_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\S_90_md5_5d18924c69519ec76993f4d58a7b2687.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\V_90_md5_eeb6b515ad2f25a3ad76099e07e030bc.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_coastal_fuelwood_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\S_90_md5_5d18924c69519ec76993f4d58a7b2687.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\W_90_md5_de1e7dc33c7227cdbcda5b7e6f9919bb.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_coastal_fwfish_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\S_90_md5_5d18924c69519ec76993f4d58a7b2687.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\X_90_md5_0cc1f3aeb8e1a566a6b220bf9986b828.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_coastal_grazing_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\S_90_md5_5d18924c69519ec76993f4d58a7b2687.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\Y_90_md5_f8393b73f3548658f610ac47acea72e7.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_coastal_marinefish_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\S_90_md5_5d18924c69519ec76993f4d58a7b2687.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\Z_90_md5_1b9d0deb1e16f6975dc3402aacf4846e.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_coastal_natureaccess_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\S_90_md5_5d18924c69519ec76993f4d58a7b2687.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A1_90_md5_1fb33de8a6ced1d1f54dcc7debed3c6c.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_coastal_nitrogen_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\S_90_md5_5d18924c69519ec76993f4d58a7b2687.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\B1_90_md5_14484122eba5a970559c57a48621d3fd.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_coastal_pollination_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\S_90_md5_5d18924c69519ec76993f4d58a7b2687.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\D1_90_md5_ee81ad59355f2309c2ecb882e788454a.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_coastal_sediment_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\S_90_md5_5d18924c69519ec76993f4d58a7b2687.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\C1_90_md5_3246d7fc06267a18f59ca9a8decf64fe.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_coastal_reeftourism_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\C1_90_md5_3246d7fc06267a18f59ca9a8decf64fe.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\Y_90_md5_f8393b73f3548658f610ac47acea72e7.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_marinefish_reeftourism_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\T_90_md5_6a0142de25bb3b5a107f7abae694c5b0.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\U_90_md5_258160b638e742e91b84979e6b2c748f.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_timber_flood_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\T_90_md5_6a0142de25bb3b5a107f7abae694c5b0.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\V_90_md5_eeb6b515ad2f25a3ad76099e07e030bc.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_timber_fuelwood_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\T_90_md5_6a0142de25bb3b5a107f7abae694c5b0.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\W_90_md5_de1e7dc33c7227cdbcda5b7e6f9919bb.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_timber_fwfish_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\T_90_md5_6a0142de25bb3b5a107f7abae694c5b0.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\Z_90_md5_1b9d0deb1e16f6975dc3402aacf4846e.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_timber_natureaccess_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\T_90_md5_6a0142de25bb3b5a107f7abae694c5b0.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A1_90_md5_1fb33de8a6ced1d1f54dcc7debed3c6c.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_timber_nitrogen_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\T_90_md5_6a0142de25bb3b5a107f7abae694c5b0.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\B1_90_md5_14484122eba5a970559c57a48621d3fd.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_timber_pollination_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\T_90_md5_6a0142de25bb3b5a107f7abae694c5b0.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\D1_90_md5_ee81ad59355f2309c2ecb882e788454a.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_timber_sediment_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\U_90_md5_258160b638e742e91b84979e6b2c748f.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\V_90_md5_eeb6b515ad2f25a3ad76099e07e030bc.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_flood_fuelwood_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\U_90_md5_258160b638e742e91b84979e6b2c748f.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\W_90_md5_de1e7dc33c7227cdbcda5b7e6f9919bb.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_flood_fwfish_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\U_90_md5_258160b638e742e91b84979e6b2c748f.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\X_90_md5_0cc1f3aeb8e1a566a6b220bf9986b828.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_flood_grazing_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\U_90_md5_258160b638e742e91b84979e6b2c748f.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\Z_90_md5_1b9d0deb1e16f6975dc3402aacf4846e.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_flood_natureaccess_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\U_90_md5_258160b638e742e91b84979e6b2c748f.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A1_90_md5_1fb33de8a6ced1d1f54dcc7debed3c6c.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_flood_nitrogen_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\U_90_md5_258160b638e742e91b84979e6b2c748f.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\B1_90_md5_14484122eba5a970559c57a48621d3fd.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_flood_pollination_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\U_90_md5_258160b638e742e91b84979e6b2c748f.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\D1_90_md5_ee81ad59355f2309c2ecb882e788454a.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_flood_sediment_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\V_90_md5_eeb6b515ad2f25a3ad76099e07e030bc.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\W_90_md5_de1e7dc33c7227cdbcda5b7e6f9919bb.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_fuelwood_fwfish_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\V_90_md5_eeb6b515ad2f25a3ad76099e07e030bc.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\Z_90_md5_1b9d0deb1e16f6975dc3402aacf4846e.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_fuelwood_natureaccess_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\V_90_md5_eeb6b515ad2f25a3ad76099e07e030bc.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A1_90_md5_1fb33de8a6ced1d1f54dcc7debed3c6c.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_fuelwood_nitrogen_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\V_90_md5_eeb6b515ad2f25a3ad76099e07e030bc.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\B1_90_md5_14484122eba5a970559c57a48621d3fd.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_fuelwood_pollination_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\V_90_md5_eeb6b515ad2f25a3ad76099e07e030bc.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\D1_90_md5_ee81ad59355f2309c2ecb882e788454a.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_fuelwood_sediment_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\W_90_md5_de1e7dc33c7227cdbcda5b7e6f9919bb.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\X_90_md5_0cc1f3aeb8e1a566a6b220bf9986b828.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_fwfish_grazing_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\W_90_md5_de1e7dc33c7227cdbcda5b7e6f9919bb.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\Z_90_md5_1b9d0deb1e16f6975dc3402aacf4846e.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_fwfish_natureaccess_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\W_90_md5_de1e7dc33c7227cdbcda5b7e6f9919bb.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A1_90_md5_1fb33de8a6ced1d1f54dcc7debed3c6c.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_fwfish_nitrogen_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\W_90_md5_de1e7dc33c7227cdbcda5b7e6f9919bb.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\B1_90_md5_14484122eba5a970559c57a48621d3fd.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_fwfish_pollination_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\W_90_md5_de1e7dc33c7227cdbcda5b7e6f9919bb.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\D1_90_md5_ee81ad59355f2309c2ecb882e788454a.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_fwfish_sediment_90.tif",
        },
       {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\X_90_md5_0cc1f3aeb8e1a566a6b220bf9986b828.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\Z_90_md5_1b9d0deb1e16f6975dc3402aacf4846e.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_grazing_natureaccess_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\X_90_md5_0cc1f3aeb8e1a566a6b220bf9986b828.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A1_90_md5_1fb33de8a6ced1d1f54dcc7debed3c6c.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_grazing_nitrogen_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\X_90_md5_0cc1f3aeb8e1a566a6b220bf9986b828.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\B1_90_md5_14484122eba5a970559c57a48621d3fd.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_grazing_pollination_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\X_90_md5_0cc1f3aeb8e1a566a6b220bf9986b828.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\D1_90_md5_ee81ad59355f2309c2ecb882e788454a.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_grazing_sediment_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\Z_90_md5_1b9d0deb1e16f6975dc3402aacf4846e.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A1_90_md5_1fb33de8a6ced1d1f54dcc7debed3c6c.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_natureaccess_nitrogen_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\Z_90_md5_1b9d0deb1e16f6975dc3402aacf4846e.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\B1_90_md5_14484122eba5a970559c57a48621d3fd.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_natureaccess_pollination_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\Z_90_md5_1b9d0deb1e16f6975dc3402aacf4846e.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\D1_90_md5_ee81ad59355f2309c2ecb882e788454a.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_natureaccess_sediment_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A1_90_md5_1fb33de8a6ced1d1f54dcc7debed3c6c.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\B1_90_md5_14484122eba5a970559c57a48621d3fd.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_nitrogen_pollination_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A1_90_md5_1fb33de8a6ced1d1f54dcc7debed3c6c.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\D1_90_md5_ee81ad59355f2309c2ecb882e788454a.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_nitrogen_sediment_90.tif",
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\B1_90_md5_14484122eba5a970559c57a48621d3fd.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\D1_90_md5_ee81ad59355f2309c2ecb882e788454a.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "overlap_pollination_sediment_90.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    #POPULATION STUFF##

    calculation_list = [ #Trying at a finer resolution - 10s A90 -- there are significantly fewer people so this is probably the correct way to do it
        #{
        #    'expression': 'raster1*(raster2)', 
        #    'symbol_to_path_map': {            
        #        'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\wgs\A_90_WARPED_near_md5_0ed997ee57533433c6372e070592e880.tif",
        #        'raster2': r"C:\Users\Becky\Documents\lspop2017_md5_eafa6a4724f3d3a6675687114d4de6ba.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
        #    'resample_method': 'near',
        #    'target_raster_path': "lspop2017_on_10sA90.tif",
        #},
        #{
        #    'expression': 'raster1*(raster2)',
        #    'symbol_to_path_map': {            
        #        'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\wgs\B_90_WARPED_near_md5_2b44cf1e234acbd8d12156068ba8ce2e.tif",
        #        'raster2': r"C:\Users\Becky\Documents\lspop2017_md5_eafa6a4724f3d3a6675687114d4de6ba.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
        #    'resample_method': 'near',
        #    'target_raster_path': "lspop2017_on_10sB90.tif",
        #},
        {
            'expression': 'raster1*(raster2)',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\wgs\C_90_WARPED_near_md5_6a33ab63b7ac8fb9a679e192741bcac5.tif",
                'raster2': r"C:\Users\Becky\Documents\lspop2017_md5_eafa6a4724f3d3a6675687114d4de6ba.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
            'resample_method': 'near',
            'target_raster_path': "lspop2017_on_10sC90.tif",
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
            'expression': 'raster1*(raster2)',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\wgs\A_90_WARPED_near_MASKED_land_2km_md5_66c8b850ace04761abef3a1d7a02f04a.tif",
                'raster2': r"C:\Users\Becky\Documents\lspop2017_md5_eafa6a4724f3d3a6675687114d4de6ba.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
            'resample_method': 'average',
            'target_raster_path': "lspop2017_on_A90.tif",
        },
        {
            'expression': 'raster1*(raster2)',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\wgs\B_90_WARPED_near_MASKED_land_2km_md5_8e7a1e1badc25b30b5dd20d9c8ae4c85.tif",
                'raster2': r"C:\Users\Becky\Documents\lspop2017_md5_eafa6a4724f3d3a6675687114d4de6ba.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
            'resample_method': 'average',
            'target_raster_path': "lspop2017_on_B90.tif",
        },
        {
            'expression': 'raster1*(raster2)',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\wgs\C_90_WARPED_near_2km_md5_f54c83a0078f91a2c5cb98c9bd23b22f.tif",
                'raster2': r"C:\Users\Becky\Documents\lspop2017_md5_eafa6a4724f3d3a6675687114d4de6ba.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
            'resample_method': 'average',
            'target_raster_path': "lspop2017_on_C90.tif",
        },
        #{
        #    'expression': 'raster1*(raster2)',
        #    'symbol_to_path_map': {            
        #        'raster1': r"C:\Users\Becky\Documents\raster_calculations\align-to-mask-and-normalize\workspace\A_90_WARPED_near_md5_1e9f19fadc8ba5e2b32c5c11bb4154cf.tif",
        #        'raster2': r"C:\Users\Becky\Documents\cnc_project\supporting_layers\poverty\chi_relative_wealth_index.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_pixel_size': (0.02222222222222399943,-0.02222222222222399943),
        #    'resample_method': 'near',
        #    'target_raster_path': "chi_relative_wealth_on_A90.tif",
        #},
        #{
        #    'expression': 'raster1*(raster2)',
        #    'symbol_to_path_map': {            
        #        'raster1': r"C:\Users\Becky\Documents\raster_calculations\align-to-mask-and-normalize\workspace\B_90_WARPED_near_md5_27f59aaa7d7e4abf71b3f80567bb66db.tif",
        #        'raster2': r"C:\Users\Becky\Documents\cnc_project\supporting_layers\poverty\chi_relative_wealth_index.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_pixel_size': (0.02222222222222399943,-0.02222222222222399943),
        #    'resample_method': 'near',
        #    'target_raster_path': "chi_relative_wealth_on_B90.tif",
        #},
        #{
        #    'expression': 'raster1*(raster2)',
        #    'symbol_to_path_map': {            
        #        'raster1': r"C:\Users\Becky\Documents\raster_calculations\align-to-mask-and-normalize\workspace\C_90_WARPED_near_md5_931c49db12100ab5837c1d0ff199f933.tif",
        #        'raster2': r"C:\Users\Becky\Documents\cnc_project\supporting_layers\poverty\chi_relative_wealth_index.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_pixel_size': (0.02222222222222399943,-0.02222222222222399943),
        #    'resample_method': 'near',
        #    'target_raster_path': "chi_relative_wealth_on_C90.tif",
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
            'expression': 'raster1*(raster2)',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\cnc_project\original_rasters\nature_access\global_normalized_people_access_lspop_2017_URCA_rural_60_noneg_md5_dcc342357e635e511e9d43ad1e057c1e.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "ruralpop_within1hr_A90.tif",
        },
        {
            'expression': 'raster1*(raster2)',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\cnc_project\original_rasters\nature_access\global_normalized_people_access_lspop_2017_URCA_urban_60_noneg_md5_24f9290d317e8985f47a8ae58b67c7b3.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "urbanpop_within1hr_A90.tif",
        },
        {
            'expression': 'raster1*(raster2)',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\cnc_project\original_rasters\nature_access\global_normalized_people_access_lspop_2017_URCA_rural_360_noneg_md5_d7b34c31cd72b84974da08471dd6620d.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "ruralpop_within6hr_A90.tif",
        },
        {
            'expression': 'raster1*(raster2)',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\cnc_project\original_rasters\nature_access\global_normalized_people_access_lspop_2017_URCA_urban_360_noneg_md5_da47d209c5ca2be346e939a4c33cf7c1.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "urbanpop_within6hr_A90.tif",
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
            'expression': 'raster1*(raster2)',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\cnc_project\original_rasters\pollination_norm\norm_ppl_fed_within_2km_per_pixel_mask_to_hab_compressed_md5_e32a0dd59de79a8dfc0d34dc08c18c41.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\wgs\A_90_WARPED_near_MASKED_land_2km_md5_66c8b850ace04761abef3a1d7a02f04a.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.002777777777777778,-0.002777777777777778),
            'resample_method': 'average',
            'target_raster_path': "pollinationpop_on_A90.tif",
        },
        #{
        #    'expression': 'raster1*(raster2)', # this is not the right way to do this!! I need the population mapped back to habitat but that seems like it hasn't been normalized correctly because it exceeds this total coastal pop
        #    'symbol_to_path_map': {            
        #        'raster1': r"C:\Users\Becky\Documents\cnc_project\original_rasters\cnc_cv\normalized_pop_on_hab\total_pop_masked_by_10m_md5_ef02b7ee48fa100f877e3a1671564be2.tif",
        #        'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\wgs\A_90_WARPED_near_MASKED_land_2km_md5_66c8b850ace04761abef3a1d7a02f04a.tif",
        #    },
        #    'target_nodata': 0,
        #    'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
        #    'resample_method': 'average',
        #    'target_raster_path': "coastalpop_on_A90.tif",
        #},
        #{
        #    'expression': 'raster1*(raster2)', #this is not actually the right way to do this - need to delineate the downstream area of A90 and then just mask to population
        #    'symbol_to_path_map': {            
        #        'raster1': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\normalized\downstream_bene_2017_hab_normalized_compressed_overviews_md5_7e8c9ecd4092068afaebc1a4b1efe3ce.tif",
        #        'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\wgs\A_90_WARPED_near_MASKED_land_2km_md5_66c8b850ace04761abef3a1d7a02f04a.tif",
        #    },
        #    'target_nodata': 0,
        #    'target_pixel_size': (0.005555555555555555768,-0.005555555555555555768),
        #    'resample_method': 'average',
        #    'target_raster_path': "downstreampop_A90.tif",
        #},
        {
            'expression': 'raster1*(raster2<2)*(raster2)',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\raster_calculations\lspop2017_compressed_md5_53e326f463a2c8a8fa92d8dea6f37df1.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\overlap\beneficiaries\downstream_mask_A_90_WARPED_near_md5_0ed997ee57533433c6372e070592e880_compressed.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
            'resample_method': 'average',
            'target_raster_path': "lspop_on_downstream_of_10sA90.tif",
        },
        {
            'expression': 'raster1*(raster2<2)*(raster2)',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\raster_calculations\lspop2017_compressed_md5_53e326f463a2c8a8fa92d8dea6f37df1.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\overlap\beneficiaries\downstream_mask_C_90_WARPED_near_md5_6a33ab63b7ac8fb9a679e192741bcac5_compressed.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
            'resample_method': 'average',
            'target_raster_path': "lspop_on_downstream_of_10sC90.tif",
        },    
        {
            'expression': 'raster1*(raster2<2)*(raster2)',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\raster_calculations\lspop2017_compressed_md5_53e326f463a2c8a8fa92d8dea6f37df1.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\overlap\beneficiaries\downstream_mask_masked_all_nathab_wstreams_esa2015_md5_c291ff6ef7db1d5ff4d95a82e0f035de.tif",
            },
            'target_nodata': 0,
            'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
            'resample_method': 'average',
            'target_raster_path': "lspop_on_downstream_of_nathab.tif", 
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    

    single_expression = {
        'expression': 'raster1*(raster2<0)',
        'symbol_to_path_map': {            
            'raster1': r"C:\Users\Becky\Documents\lspop2017_md5_eafa6a4724f3d3a6675687114d4de6ba.tif",
            'raster2': r"C:\Users\Becky\Documents\cnc_project\supporting_layers\poverty\chi_relative_wealth_index.tif",
        },
        'target_nodata': -9999,
        'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
        'resample_method': 'near',
        'target_raster_path': "lspop_negchi.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [ 
        {
            'expression': '(raster1<0)*raster2',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\cnc_project\overlap\population\chi_relative_wealth_on_A90.tif",
                'raster2': r"C:\Users\Becky\Documents\lspop2017_md5_eafa6a4724f3d3a6675687114d4de6ba.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
            'resample_method': 'near',
            'target_raster_path': "pop_negchi_on_A90.tif",
        },
        {
            'expression': '(raster1<0)*raster2',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\cnc_project\overlap\population\chi_relative_wealth_on_B90.tif",
                'raster2': r"C:\Users\Becky\Documents\lspop2017_md5_eafa6a4724f3d3a6675687114d4de6ba.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
            'resample_method': 'near',
            'target_raster_path': "pop_negchi_on_B90.tif",
        },
        {
            'expression': '(raster1<0)*raster2',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\cnc_project\overlap\population\chi_relative_wealth_on_C90.tif",
                'raster2': r"C:\Users\Becky\Documents\lspop2017_md5_eafa6a4724f3d3a6675687114d4de6ba.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.008333333333333333218,-0.008333333333333333218),
            'resample_method': 'near',
            'target_raster_path': "pop_negchi_on_C90.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    


    #For Pat's analysis

    #This doesn't work anymore, something about bounding box not fitting... so use cna_align_to_mask.py instead:
    #MASK_ECOSHARD_URL = ('https://storage.googleapis.com/critical-natural-capital-ecoshards/habmasks/landmask_0s_2km_moisturextent_md5_b91bdc0eed9397d0ed104be8cb145880.tif')
    #RASTER_LIST= [('A_90_md5_79f5e0d5d5029d90e8f10d5932da93ff.tif', True, False),]
    #RESAMPLE_MODE = 'near'
    #it yields A_90_WARPED_near_MASKED_md5_66c8b850ace04761abef3a1d7a02f04a.tif which I renamed A_90_WARPED_near_MASKED_land0s_2km_md5_66c8b850ace04761abef3a1d7a02f04a.tif

#    wgs84_srs = osr.SpatialReference()
#    wgs84_srs.ImportFromEPSG(4326)    
#
#    single_expression = { 
#        'expression': 'raster1*(raster2>-1)',
#        'symbol_to_path_map': {            
#            #'raster1': r"C:\Users\Becky\Documents\raster_calculations\align-to-mask-and-normalize\workspace\A_90_WARPED_near_md5_1e9f19fadc8ba5e2b32c5c11bb4154cf.tif",
#            'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\A_90.tif",
#            'raster2': r"C:\Users\Becky\Documents\cnc_project\supporting_layers\landmask_0s_2km_moisturextent.tif",
#        },
#        'target_nodata': -9999,
#        'target_projection_wkt': wgs84_srs.ExportToWkt(),
#        'target_pixel_size': (0.02131900000000000114,-0.02131900000000000114),
#        'resample_method': 'near',
#        'target_raster_path': "A_90_land_nathab.tif",
#    }
#
#    raster_calculations_core.evaluate_calculation(
#        single_expression, TASK_GRAPH, WORKSPACE_DIR)
#
#    TASK_GRAPH.join()
#    TASK_GRAPH.close()
#
#    return

    #then use nodata_replace using landmask_0s_2km_moisturextent and then do the below on that resulting raster
    # python nodata_replace.py "C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\wgs\A_90_WARPED_near_MASKED_land_2km_md5_66c8b850ace04761abef3a1d7a02f04a.tif" "C:\Users\Becky\Documents\cnc_project\supporting_layers\landmask_0s_2km_moisturextent_md5_b91bdc0eed9397d0ed104be8cb145880.tif" A_90_land_nodata0s.tif

    single_expression = {
        'expression': '(raster1)*(raster2>-6)',
        'symbol_to_path_map': {            
            'raster1': r"C:\Users\Becky\Documents\raster_calculations\A_90_land_nodata0s.tif",
            'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\realized_e_source_ratio_ann_mean.tif",
        },
        'target_nodata': -9999,
        'target_pixel_size': (1.493750063578287657,-1.486111252396195015),
        'resample_method': 'average',
        'target_raster_path': "A_90_1.5d_prop_area.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': 'raster1-raster2',
        'symbol_to_path_map': {            
            'raster1': r"C:\Users\Becky\Downloads\cntr_2km_nocarb_land_resampled15_mode.tif",
            'raster2': r"C:\Users\Becky\Documents\raster_calculations\align-to-mask-and-normalize\workspace\solution_A_all_targets_resampled1.5d_near_md5_98d52ff13ca9a38784a687339e30b2fd.tif",
        },
        'target_nodata': -9999,
        'target_pixel_size': (1.493750063578287657,-1.486111252396195015),
        'resample_method': 'near',
        'target_raster_path': "diff_oldcna_newcna_1.5d.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    calculation_list = [ #scrubbin out the accidental negatives from nodata overlap in the nature access
        {
            'expression': '(raster1>=0)*raster1 + (raster1<0)*0',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\raster_calculations\align_to_mask_workspace\ecoshards\global_normalized_people_access_lspop_2017_URCA_rural_60.0m_md5_77e111769dcab34cf992fb0d3a9eb49c.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "global_normalized_people_access_lspop_2017_URCA_rural_60_noneg.tif",
        },
        {
            'expression': '(raster1>=0)*raster1 + (raster1<0)*0',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\raster_calculations\align_to_mask_workspace\ecoshards\global_normalized_people_access_lspop_2017_URCA_rural_360.0m_md5_5cd804c489ab949c4891410d65b71057.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "global_normalized_people_access_lspop_2017_URCA_rural_360_noneg.tif",
        },
        {
            'expression': '(raster1>=0)*raster1 + (raster1<0)*0',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\raster_calculations\align_to_mask_workspace\ecoshards\global_normalized_people_access_lspop_2017_URCA_urban_60.0m_md5_77d3af07d88721543128205645f75b8d.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "global_normalized_people_access_lspop_2017_URCA_urban_60_noneg.tif",
        },
        {
            'expression': '(raster1>=0)*raster1 + (raster1<0)*0',
            'symbol_to_path_map': {            
                'raster1': r"C:\Users\Becky\Documents\raster_calculations\align_to_mask_workspace\ecoshards\global_normalized_people_access_lspop_2017_URCA_urban_360.0m_md5_e7720b3032df6ea8293cddcb2be26802.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "global_normalized_people_access_lspop_2017_URCA_urban_360_noneg.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    

    
    calculation_list = [ #making masks so I can try average_rasters on these and masked_all_nathab_wstreams_esa2015_nodata_WARPED_near_md5_d801fffb0e3fbfd8d7ffb508f18ebb7c.tif to see where these exist outside that mask (they shouldn't, but they do)
        {
            'expression': 'raster1*10', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\resampled_Eckert2km\masked_all_nathab_wstreams_esa2015_nodata_WARPED_near_md5_d801fffb0e3fbfd8d7ffb508f18ebb7c.tif",
             },
            'target_nodata': 0,
            'target_raster_path': "nathab_10.tif",
        },
        {
            'expression': 'raster1*2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\churn\target_stitch_dir\A_100_md5_7474de70786c3dce0b760c691368c839.tif",
             },
            'target_nodata': 0,
            'target_raster_path': "A100_2.tif",
        },
        {
            'expression': 'raster1*3', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\churn\target_stitch_dir\A_90_md5_396196b740bcbb151e033ff9f9609fe5.tif",
             },
            'target_nodata': 0,
            'target_raster_path': "A90_3.tif",
        },        
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    calculation_list = [ #making a realized sediment layer that's not masked to nathab, for a presentation
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\original_rasters\potential_sedimentdeposition_md5_aa9ee6050c423b6da37f8c2723d9b513.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\stream_attenuated\downstream_bene_2017_500000.0_compressed_overviews_md5_a73557e0c216e390d4e288816c9838bb.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.005555555555555556, -0.005555555555555556),
            'resample_method': 'near',
            'target_raster_path': "realized_sediment_attn_500k.tif",
        },        
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    #marinefish problems

    single_expression = {
        'expression': '(raster1>=0)', 
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\solutions\D-I1_90s\Y_90_md5_68117f49cd41e41f3a8915a2a8c941b1.tif",
        },
        'target_nodata': 0,
        'target_raster_path': "marinefish_extent_Eckert.tif", #this is not a good mask to use because the optimization created haloes around some islands
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    #create mask by rasterizing EEZ vector and then use align_to_mask but without using a mask to align that mask to Eckert 2km

    single_expression = {
        'expression': 'raster1*(raster2>0)',
        'symbol_to_path_map': {            
            'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\Eckert\90_targets\Y_90_md5_81cd585dcfadd703e24c0a9229c1cdc9.tif",
            'raster2': r"C:\Users\Becky\Documents\cnc_project\supporting_layers\eez_mask_eckert_2km_md5_3208b8094dbece295374bddf4d99d192.tif",
        },
        'target_nodata': 0,
        'target_raster_path': "Y_90_md5_81cd585dcfadd703e24c0a9229c1cdc9_nodata0.tif", #still has haloes but at least the mask is the full EEZ area
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    #this is the way to fix it:
    single_expression = {
        'expression': '(raster1>0)*0 + (raster1<1)*-9999',
        'symbol_to_path_map': {            
            'raster1': r"C:\Users\Becky\Documents\raster_calculations\align_to_mask_workspace\ecoshards\eez_mask_eckert_2km_md5_3208b8094dbece295374bddf4d99d192.tif",
        },
        'target_nodata': -9999,
        'target_raster_path': "eez_mask0s_eckert_2km.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

#    then do python nodata_replace to fill the nodata haloes so that the whole EEZ can show up as CNA area
# python nodata_replace.py "C:\Users\Becky\Documents\cnc_project\resampled_Eckert2km\realized_marinefish_watson_2010_2014_clamped_WARPED_average_MASKED_md5_1c9ea302eeadd8027f6a17e03f943888.tif" "C:\Users\Becky\Documents\cnc_project\supporting_layers\eez_mask0s_eckert_2km_md5_72e7907ce7380f95e20d3c2b4448605b.tif" realized_marinefish_watson_2010_2014_clamped_0sfill_WARPED_average_MASKED.tif
# python nodata_replace.py "C:\Users\Becky\Documents\cnc_project\optimization\critical-natural-capital-optimizations\stitched_solutions\8-21\Y_90_md5_f8393b73f3548658f610ac47acea72e7.tif" "C:\Users\Becky\Documents\cnc_project\supporting_layers\eez_mask0s_eckert_2km_md5_72e7907ce7380f95e20d3c2b4448605b.tif" Y_90_0sfill.tif




    NNth_fl = 12445
    clamped_service_list = [ #some services just have crazy high values that throw the whole percentiles off so we're clamping them to the 99th percentile
        {
            'expression': f'(service>{NNth_fl})*{NNth_fl} + (service<={NNth_fl})*(service>=0)*service + -9999*(service<0)', #sets anything above the 99th percentile value to that value, anything negative to nodata
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\needed_clamping\realized_floodmitigation_attn_50km_nathab_md5_3cbadb2d1b4207f029a264e090783c6d.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_floodmitigation_attn_50km_nathab_clamped.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
    ]

    for calculation in clamped_service_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': 'service*mask + (mask<1)*-9999', 
            'symbol_to_path_map': {
                'mask': r"C:\Users\Becky\Documents\cnc_project\supporting_layers\masked_all_nathab_wstreams_esa2015_md5_c291ff6ef7db1d5ff4d95a82e0f035de.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\stream_attenuated\service\realized_floodmitigation_attn_500km_md5_1b659e3fd93e5f0b6aac396245258517.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_floodmitigation_attn_500km_nathab.tif",
            'target_pixel_size': (0.002777777777777778, -0.002777777777777778),
            'resample_method': 'near',
        },
        {
            'expression': 'service*mask + (mask<1)*-9999', 
            'symbol_to_path_map': {
                'mask': r"C:\Users\Becky\Documents\cnc_project\supporting_layers\masked_all_nathab_wstreams_esa2015_md5_c291ff6ef7db1d5ff4d95a82e0f035de.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\stream_attenuated\service\realized_floodmitigation_attn_50km_md5_029cbd998fc4464cf04861cf58dddc1d.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_floodmitigation_attn_50km_nathab.tif",
            'target_pixel_size': (0.002777777777777778, -0.002777777777777778),
            'resample_method': 'near',
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
            'expression': 'service*benes', 
            'symbol_to_path_map': {
                'benes': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\stream_attenuated\downstream_bene_2017_500000.0_compressed_overviews_md5_a73557e0c216e390d4e288816c9838bb.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\potential_nitrogenretention_nci_unmasked_md5_09425dff042ea8dbb94a8d1977be472a.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_nitrogenretention_unmasked_attn_500k.tif",
            'target_pixel_size': (0.002777777777777778, -0.002777777777777778),
            'resample_method': 'near',
        },
        {
            'expression': 'service*benes', 
            'symbol_to_path_map': {
                'benes': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\stream_attenuated\downstream_bene_2017_50000.0_compressed_overviews_md5_ddbc9006bbfb21ef681a42bf78046b69.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\potential_nitrogenretention_nci_unmasked_md5_09425dff042ea8dbb94a8d1977be472a.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_nitrogenretention_unmasked_attn_50k.tif",
            'target_pixel_size': (0.002777777777777778, -0.002777777777777778),
            'resample_method': 'near',
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    calculation_list = [
#        {
#            'expression': 'raster1*raster2', 
#            'symbol_to_path_map': {
#                'raster1': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\need_processing\potential_nitrogenretention_nci_nathab_clamped_md5_fff6f944bfaf13baf24129f7fbfb1107.tif",
#                'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\stream_attenuated\downstream_bene_2017_50000.0_compressed_overviews_md5_ddbc9006bbfb21ef681a42bf78046b69.tif",
#            },
#            'target_nodata': -9999,
#            'target_raster_path': "realized_nitrogenretention_attn_50km.tif",
#            'target_pixel_size': (0.002777777777777778, -0.002777777777777778),
#            'resample_method': 'near',
#        },
#        {
#            'expression': 'raster1*raster2', 
#            'symbol_to_path_map': {
#                'raster1': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\need_processing\potential_sedimentdeposition_nathab_clamped_md5_1d826c8885c6479b6307bc345b95d8bf.tif",
#                'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\stream_attenuated\downstream_bene_2017_50000.0_compressed_overviews_md5_ddbc9006bbfb21ef681a42bf78046b69.tif",
#            },
#            'target_nodata': -9999,
#            'target_raster_path': "realized_sedimentdeposition_attn_50km.tif",
#            'target_pixel_size': (0.002777777777777778, -0.002777777777777778),
#            'resample_method': 'near',
#        },
#        {
#            'expression': 'raster1*raster2', 
#            'symbol_to_path_map': {
#                'raster1': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\need_processing\potential_nitrogenretention_nci_nathab_clamped_md5_fff6f944bfaf13baf24129f7fbfb1107.tif",
#                'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\stream_attenuated\downstream_bene_2017_500000.0_compressed_overviews_md5_a73557e0c216e390d4e288816c9838bb.tif",
#            },
#            'target_nodata': -9999,
#            'target_raster_path': "realized_nitrogenretention_attn_500km.tif",
#            'target_pixel_size': (0.002777777777777778, -0.002777777777777778),
#            'resample_method': 'near',
#        },
#        {
#            'expression': 'raster1*raster2', 
#            'symbol_to_path_map': {
#                'raster1': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\need_processing\potential_sedimentdeposition_nathab_clamped_md5_1d826c8885c6479b6307bc345b95d8bf.tif",
#                'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\stream_attenuated\downstream_bene_2017_500000.0_compressed_overviews_md5_a73557e0c216e390d4e288816c9838bb.tif",
#            },
#            'target_nodata': -9999,
#            'target_raster_path': "realized_sedimentdeposition_attn_500km.tif",
#            'target_pixel_size': (0.002777777777777778, -0.002777777777777778),
#            'resample_method': 'near',
#        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\original_rasters\potential_floodmitigation_PotInflGStorage.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\stream_attenuated\downstream_bene_floodplain_500000.0_compressed_overviews_md5_2ce1f378646fcfe8c9ddf98bd6212d03.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_floodmitigation_attn_500km.tif",
            'target_pixel_size': (0.002777777777777778, -0.002777777777777778),
            'resample_method': 'near',
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\original_rasters\potential_floodmitigation_PotInflGStorage.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\stream_attenuated\downstream_bene_floodplain_50000.0_compressed_overviews_md5_6c604be0fc0d87225dd81adeeb4b67a3.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_floodmitigation_attn_50km.tif",
            'target_pixel_size': (0.002777777777777778, -0.002777777777777778),
            'resample_method': 'near',
        },
        
        #{
        #    'expression': 'raster1*raster2', 
        #    'symbol_to_path_map': {
        #        'raster1': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\need_processing\potential_nitrogenretention_nci_nathab_clamped_md5_fff6f944bfaf13baf24129f7fbfb1107.tif",
        #        'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\downstream_bene_2017_compressed_overviews_md5_32c17fb4ab0eb2b1fe193839dbc7e85b.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_raster_path': "realized_nitrogenretention_0attn.tif",
        #    'target_pixel_size': (0.002777777777777778, -0.002777777777777778),
        #    'resample_method': 'near',
        #},
        #{
        #    'expression': 'raster1*raster2', 
        #    'symbol_to_path_map': {
        #        'raster1': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\need_processing\potential_sedimentdeposition_nathab_clamped_md5_1d826c8885c6479b6307bc345b95d8bf.tif",
        #        'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\downstream_bene_2017_compressed_overviews_md5_32c17fb4ab0eb2b1fe193839dbc7e85b.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_raster_path': "realized_sedimentdeposition_0attn.tif",
        #    'target_pixel_size': (0.002777777777777778, -0.002777777777777778),
        #    'resample_method': 'near',
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
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\need_processing\potential_nitrogenretention_nci_nathab_clamped_md5_fff6f944bfaf13baf24129f7fbfb1107.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\stream_attenuated\downstream_bene_2017_0.9999_normalized_compressed_overviews_md5_afbbfe893a6fb155aa6fffc54c6e8b69.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_nitrogenretention_attn_0.9999.tif",
            'target_pixel_size': (0.002777777777777778, -0.002777777777777778),
            'resample_method': 'near',
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\need_processing\potential_sedimentdeposition_nathab_clamped_md5_1d826c8885c6479b6307bc345b95d8bf.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\stream_attenuated\downstream_bene_2017_0.9999_normalized_compressed_overviews_md5_afbbfe893a6fb155aa6fffc54c6e8b69.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_sedimentdeposition_attn_0.9999.tif",
            'target_pixel_size': (0.002777777777777778, -0.002777777777777778),
            'resample_method': 'near',
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\need_processing\potential_nitrogenretention_nci_nathab_clamped_md5_fff6f944bfaf13baf24129f7fbfb1107.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\stream_attenuated\downstream_bene_2017_0.999_compressed_overviews_md5_d15639dbfd5914f44c59642c459b6ced.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_nitrogenretention_attn_0.999.tif",
            'target_pixel_size': (0.002777777777777778, -0.002777777777777778),
            'resample_method': 'near',
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\need_processing\potential_sedimentdeposition_nathab_clamped_md5_1d826c8885c6479b6307bc345b95d8bf.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\stream_attenuated\downstream_bene_2017_0.999_compressed_overviews_md5_d15639dbfd5914f44c59642c459b6ced.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_sedimentdeposition_attn_0.999.tif",
            'target_pixel_size': (0.002777777777777778, -0.002777777777777778),
            'resample_method': 'near',
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return
    

    masked_service_list = [
        {
            'expression': 'service*mask', 
            'symbol_to_path_map': {
                'mask': r"C:\Users\Becky\Documents\cnc_project\supporting_layers\EEZ_mask_0027_compressed_md5_0f25e6a690fef616d34c5675b57e76f8.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\realized_coastalprotection_norm_md5_485aef1d6c412bde472bdaa1393100d7.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_coastalprotection_norm_offshore.tif",
            'target_pixel_size': (0.002777777777777778, -0.002777777777777778),
            'resample_method': 'near',
        },
    ]

    for calculation in masked_service_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': '(raster1>0)*raster1*(raster2>0)*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\original_rasters\cnc_cv\global_cv_pop_md5_d7af43a2656b44838f01796f523fb696.tif",
            'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\cnc_cv\global_cv_value_md5_0f6c1b3a2904d7de5c263490814c4a44.tif",
        },
        'target_nodata': -9999,
        'target_pixel_size': (0.0027777777777777778, -0.0027777777777777778),
        'resample_method': 'near',
        'target_raster_path': "realized_coastalprotection_norm.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    expression_list = [
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\need_processing\potential_nitrogenretention_nci_nathab_clamped_md5_0403ac4f961b259a89c013d939c39463.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\downstream_bene_2017_normalized_compressed_overviews_md5_0da01aaa9d5d03c652a03b64afde24f8.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_nitrogenretention_norm.tif",
            'target_pixel_size': (0.002777777777777778, -0.002777777777777778),
            'resample_method': 'near',
        },
        {
            'expression': 'raster1*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\need_processing\potential_sedimentdeposition_nathab_clamped_md5_1d826c8885c6479b6307bc345b95d8bf.tif",
                'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\downstream_bene_2017_normalized_compressed_overviews_md5_0da01aaa9d5d03c652a03b64afde24f8.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_sedimentdeposition_norm.tif",
            'target_pixel_size': (0.002777777777777778, -0.002777777777777778),
            'resample_method': 'near',
        },
    ]

    for calculation in expression_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': 'raster1*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\MarkMulligansLayer\acc_gr_storage_ratio__lt_10_globally.tif",
            'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\floodplains\downstream_bene_floodplain_hab_normalized_compressed_overviews_md5_07d02a635bc908fed74d0a6e73152dc6.tif",
        },
        'target_nodata': -9999,
        'target_pixel_size': (0.0027777777777777778, -0.0027777777777777778),
        'resample_method': 'near',
        'target_raster_path': "realized_floodmitigation_norm.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    #first make this mask. then upload it to a bucket. then use align to mask and normalize to reproject it in Eckert 2km.
    #then use that mask to remask/project all of the layers in align to mask and normalize. do Mark's both ways

    single_expression = {
        'expression': 'raster1*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\supporting_layers\masked_all_nathab_wstreams_esa2015_md5_c291ff6ef7db1d5ff4d95a82e0f035de.tif",
            'raster2': r"C:\Users\Becky\Documents\cnc_project\supporting_layers\landmask_10s_md5_748981cbf6ebf22643a3a3e655ec50ce.tif",
        },
        'target_nodata': 0,
        'target_pixel_size': (0.0027777777777777778, -0.0027777777777777778),
        'resample_method': 'near',
        'target_raster_path': "masked_all_nathab_wstreams_esa2015_nodata.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    

    #99.9th percentiles of the new layers (NCI nitrogen retention and new normalized pollination)
    NNth_nit = 2325 
    NNth_poll = 48
    NNth_fl = 31755
    clamped_service_list = [ #some services just have crazy high values that throw the whole percentiles off so we're clamping them to the 99th percentile
        {
            'expression': f'(service>{NNth_fl})*{NNth_fl} + (service<={NNth_fl})*(service>=0)*service + -9999*(service<0)', #sets anything above the 99th percentile value to that value, anything negative to nodata
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\needed_clamping\realized_floodmitigation_attn_500km_nathab_md5_bc788aea3fd99c82ef38b51693fc2ed5.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_floodmitigation_attn_500km_nathab.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>{NNth_nit})*{NNth_nit} + (service<={NNth_nit})*(service>=0)*service + -9999*(service<0)', #sets anything above the 99th percentile value to that value, anything negative to nodata
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\needed_clamping\potential_nitrogenretention_nci_nathab.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "potential_nitrogenretention_nci_nathab_clamped.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>{NNth_poll})*({NNth_poll})+(service<={NNth_poll})*(service>=0)*service + -9999*(service<0)',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\needed_clamping\norm_ppl_fed_within_2km_per_pixel_mask_to_hab_compressed_md5_e32a0dd59de79a8dfc0d34dc08c18c41.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_pollination_norm_nathab_clamped.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
    ]

    for calculation in clamped_service_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    single_expression = {
        'expression': 'raster2/raster1',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\MarkMulligansLayer\acc_gr_storage_ratio__lt_10_globally.tif",
            'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\MarkMulligansLayer\RealInflGStoragePop.tif",
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'default_inf': -9999,
        'target_pixel_size': (0.0833333333333333, -0.0833333333333333),
        'resample_method': 'near',
        'target_raster_path': "backcalculated_MM_downstreambenes.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': '(raster1>0)*raster1*(raster2>0)*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\original_rasters\cnc_cv\global_cv_pop_md5_d7af43a2656b44838f01796f523fb696.tif",
            'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\cnc_cv\global_cv_value_md5_0f6c1b3a2904d7de5c263490814c4a44.tif",
        },
        'target_nodata': -9999,
        'target_pixel_size': (0.0027777777777777778, -0.0027777777777777778),
        'resample_method': 'near',
        'target_raster_path': "realized_coastalprotection_norm.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return
    
    masked_service_list = [
        {
            'expression': '(raster1>=0)*raster2', 
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\floodplains\global_floodplains_mask.tif",
                'raster2': r"C:\Users\Becky\Documents\lspop2017_md5_eafa6a4724f3d3a6675687114d4de6ba.tif",
            },
            'target_nodata': -2147483647,
            'target_raster_path': "floodplains_masked_pop_30s.tif",
            'target_pixel_size': (0.008333333333333333, -0.008333333333333333),
            'resample_method': 'near',
        },
    ]

    for calculation in masked_service_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': '(raster1>0)*raster1*raster2 + (raster1<=0)*-1',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\supporting_layers\masked_all_nathab_esa2015_md5_50debbf5fba6dbdaabfccbc39a9b1670.tif",
            'raster2':r"C:\Users\Becky\Documents\cnc_project\original_rasters\potential_nitrogenretention_nci_md5_09425dff042ea8dbb94a8d1977be472a.tif",
        },
        'target_nodata': -1,
        'target_pixel_size': (0.0027777777777777778, -0.0027777777777777778),
        'resample_method': 'near',
        'target_raster_path': "potential_nitrogenretention_nci_nathab.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': 'raster1-raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\nci\ndr\compressed_baseline_currentpractices_300.0_D8_modified_load_md5_a836509e72dacd536764249ea7beb4d7.tif",
            'raster2':r"C:\Users\Becky\Documents\nci\ndr\compressed_baseline_currentpractices_300.0_D8_export_md5_eb9855f076fdc8d45a42ca45b5c23219.tif",
        },
        'target_nodata': -1,
        'target_pixel_size': (0.0027777777777777778, -0.0027777777777777778),
        'resample_method': 'near',
        'target_raster_path': "potential_nitrogenretention_nci.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': '(raster1/raster2)*raster3*(raster2>0)',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\downstream_bene_2017_compressed_overviews_md5_a2d9f969617c728311b4f3d33bc5f1f8.tif",
            'raster2':r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\global_stitch_dsbsum_md5_55441129edcc27880861bf448309481a.tif",
            'raster3':r"C:\Users\Becky\Documents\cnc_project\original_rasters\downstream_beneficiaries\global_stitch_lspopsum_md5_a2db49316a2d47840a9a8f17657fff3b.tif",
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_pixel_size': (0.005555555555, -0.005555555555),
        'resample_method': 'near',
        'target_raster_path': "downstream_bene_2017_norm.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return
    

    

    single_expression = {
        'expression': '(raster1>=8)*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\supporting_layers\Urban-Rural Catchment Areas_md5_942ffae026b526a1044680e28ef58b89.tif",
            'raster2':r"C:\Users\Becky\Documents\lspop2017_md5_eafa6a4724f3d3a6675687114d4de6ba.tif",
        },
        'target_nodata': -1,
        'target_pixel_size': (0.0083333333, 0.0083333333),
        'resample_method': 'near',
        'target_raster_path': "lspop_2017_URCA_rural.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return
    
    single_expression = {
        'expression': '(raster1<8)*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\supporting_layers\Urban-Rural Catchment Areas_md5_942ffae026b526a1044680e28ef58b89.tif",
            'raster2':r"C:\Users\Becky\Documents\lspop2017_md5_eafa6a4724f3d3a6675687114d4de6ba.tif",
        },
        'target_nodata': -1,
        'target_pixel_size': (0.0083333333, 0.0083333333),
        'resample_method': 'near',
        'target_raster_path': "lspop_2017_URCA_urban.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    #unfortunately the VuC is nodata in a bunch of places where we have CNA. So need to go back and nodata_replace copy b to where a is nodata:
    # python nodata_replace.py [raster_a_path] [raster_b_path] [target_path]
    # python nodata_replace.py "C:\Users\Becky\Documents\cnc_project\overlap\ctr90_outside_VuC_2km.tif" "C:\Users\Becky\Documents\cnc_project\optimization\ctr90_2km_VuCextent.tif" "full_cntr90_outside_VuC_2km.tif"
    # so actually we need to go back and fill all the nodata in the Vulnerable Carbon layer with 0's instead of nodata
    # python nodata_replace.py "C:\Users\Becky\Documents\cnc_project\supporting_layers\carbon\VuC_top90_2km.tif" "C:\Users\Becky\Documents\cnc_project\supporting_layers\carbon\landmask_0s_2km_VuCextent.tif" "VuC_top90_2km_0s.tif"
    # python nodata_replace.py "C:\Users\Becky\Documents\cnc_project\overlap\moisture_top90_2km_ext.tif" "C:\Users\Becky\Documents\cnc_project\supporting_layers\landmask_0s_2km_moisturextent.tif" "moisture_top90_2km_0s.tif"


    single_expression = {
        'expression': '(raster1>0)*(raster2<1)',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\supporting_layers\masked_all_nathab_esa2015_md5_50debbf5fba6dbdaabfccbc39a9b1670.tif",
            'raster2':r"C:\Users\Becky\Documents\cnc_project\supporting_layers\landmask_0s_2km.tif",
        },
        'target_nodata': -1,
        'target_pixel_size': (0.021319, 0.021319),
        'resample_method': 'near',
        'target_raster_path': "natural_assets_wostreams_300m_to_2km_0s.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)    

    single_expression = {
        'expression': '(raster1>0)*(raster2<1)',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\prioritiz-2km-country\cntr_2km_carb_wgs.tif",
            'raster2':r"C:\Users\Becky\Documents\cnc_project\supporting_layers\landmask_0s_2km.tif",
        },
        'target_nodata': -1,
        'target_projection_wkt': wgs84_srs.ExportToWkt(),
        'target_pixel_size': (0.021319, 0.021319),
        'resample_method': 'near',
        'target_raster_path': "natural_assets_full_2km_0s.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': '(raster1>0)*(raster2<1)',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\supporting_layers\masked_all_nathab_wstreams_esa2015_md5_c291ff6ef7db1d5ff4d95a82e0f035de.tif",
            'raster2':r"C:\Users\Becky\Documents\cnc_project\supporting_layers\landmask_0s_2km.tif",
        },
        'target_nodata': -1,
        'target_pixel_size': (0.021319, 0.021319),
        'resample_method': 'near',
        'target_raster_path': "natural_assets_300m_to_2km_0s.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    single_expression = {
        'expression': '(raster1==1)',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\overlap\nonoverlapping_ctr90_moisture_VuC_2km_0s.tif",
        },
        'target_nodata': -9999,
        'target_raster_path': "ctr90_outside_VuC_moisture_2km_0s.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': '(raster1<1)*(raster4>2)',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\overlap\ctr90_VuCtop90_2km_0s.tif",
            'raster4': r"C:\Users\Becky\Documents\cnc_project\optimization\prioritiz-2km-country\cntr_2km_nocarb.tif",
        },
        'target_nodata': -9999,
        'target_pixel_size': (0.021319, 0.021319),
        'resample_method': 'near',
        'target_raster_path': "ctr90_outside_VuC_2km_0s.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': '(raster1<1)*(raster2<1)*(raster3<1)*(raster4>2) + 2*(raster1<1)*(raster2<1)*(raster3<1)*raster5 + 3*(raster1<1)*(raster2<1)*(raster3<1)*raster6 ',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\overlap\ctr90_VuCtop90_2km_0s.tif",
            'raster2': r"C:\Users\Becky\Documents\cnc_project\overlap\ctr90_moisturetop90_2km_0s.tif",
            'raster3': r"C:\Users\Becky\Documents\cnc_project\overlap\ctr90_moisture_VuC_2km_0s.tif",
            'raster4': r"C:\Users\Becky\Documents\cnc_project\optimization\prioritiz-2km-country\cntr_2km_nocarb.tif",
            'raster5': r"C:\Users\Becky\Documents\cnc_project\overlap\moisture_top90_2km_0s.tif",
            'raster6': r"C:\Users\Becky\Documents\cnc_project\supporting_layers\carbon\VuC_top90_2km_0s.tif",
        },
        'target_nodata': -9999,
        'target_pixel_size': (0.021319, 0.021319),
        'resample_method': 'near',
        'target_raster_path': "nonoverlapping_ctr90_moisture_VuC_2km_0s.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': 'raster1*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\overlap\ctr90_VuCtop90_2km_0s.tif",
            'raster2': r"C:\Users\Becky\Documents\cnc_project\overlap\ctr90_moisturetop90_2km_0s.tif",
        },
        'target_nodata': -9999,
        'target_pixel_size': (0.021319, 0.021319),
        'resample_method': 'near',
        'target_raster_path': "ctr90_moisture_VuC_2km_0s.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': '(raster1>2)*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\prioritiz-2km-country\cntr_2km_nocarb.tif",
            'raster2': r"C:\Users\Becky\Documents\cnc_project\overlap\moisture_top90_2km_0s.tif",
        },
        'target_nodata': -9999,
        'target_pixel_size': (0.021319, 0.021319),
        'resample_method': 'near',
        'target_raster_path': "ctr90_moisturetop90_2km_0s.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': '(raster1>2)*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\prioritiz-2km-country\cntr_2km_nocarb.tif",
            'raster2': r"C:\Users\Becky\Documents\cnc_project\supporting_layers\carbon\VuC_top90_2km_0s.tif",
        },
        'target_nodata': 66535,
        'target_pixel_size': (0.021319, 0.021319),
        'resample_method': 'near',
        'target_raster_path': "ctr90_VuCtop90_2km_0s.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': '(raster1<2)',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\overlap\nonoverlapping_ctr90_moisture_VuC_2km.tif",
        },
        'target_nodata': -9999,
        'target_raster_path': "ctr90_2km.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    single_expression = {
        'expression': '(raster1>2)*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\prioritiz-2km-country\cntr_2km_nocarb.tif",
            'raster2': r"C:\Users\Becky\Documents\cnc_project\overlap\moisture_top90.tif",
        },
        'target_nodata': -9999,
        'target_pixel_size': (0.021319, 0.021319),
        'resample_method': 'near',
        'target_raster_path': "ctr90_moisturetop90_2km.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return
    

    single_expression = {
        'expression': '(raster1>2.61)',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\resampled_30s\realized_moisturerecycling_nathab30s.tif",
        },
        'target_nodata': -9999,
        'target_raster_path': "moisture_top90.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': '(raster2>=0)*raster1',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\raster_calculations\VuC_top90.tif",
            'raster2': r"C:\Users\Becky\Documents\raster_calculations\ctr90_VuCtop90_2km.tif",
        },
        'target_nodata': 66535,
        'target_pixel_size': (0.021319, 0.021319),
        'resample_method': 'near',
        'target_raster_path': "VuC_top90_2km.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return
    
    single_expression = {
        'expression': '(raster1>2)*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\prioritiz-2km-country\cntr_2km_nocarb.tif",
            'raster2': r"C:\Users\Becky\Documents\cnc_project\supporting_layers\carbon\VuC_top90.tif",
        },
        'target_nodata': 66535,
        'target_pixel_size': (0.021319, 0.021319),
        'resample_method': 'near',
        'target_raster_path': "ctr90_VuCtop90_2km.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': '(raster1>37)',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\supporting_layers\carbon\Vulnerable_C_Total_2018.tif",
        },
        'target_nodata': 65535,
        'target_raster_path': "VuC_top90.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': 'raster1*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\ctr90_moisture61_2km.tif",
            'raster2': r"C:\Users\Becky\Documents\cnc_project\optimization\ctr90_C65_2km.tif",
        },
        'target_nodata': -9999,
        'target_pixel_size': (0.021319, 0.021319),
        'resample_method': 'near',
        'target_raster_path': "ctr90_moisture61_C65_2km.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': '(raster1>2)*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\prioritiz-2km-country\cntr_2km_nocarb.tif",
            'raster2': r"C:\Users\Becky\Documents\raster_calculations\moisture_top39.tif",
        },
        'target_nodata': -9999,
        'target_pixel_size': (0.021319, 0.021319),
        'resample_method': 'near',
        'target_raster_path': "ctr90_moisture61_2km.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': '(raster1>4.55)',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\resampled_30s\realized_moisturerecycling_nathab30s.tif",
        },
        'target_nodata': -9999,
        'target_raster_path': "moisture_top39.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    raster_calculation_list = [
        {
            'expression': 'raster1>0.9',
            'symbol_to_path_map': { 
                'raster1': r"C:\Users\Becky\Documents\cnc_project\output_CBD\food_water_average_raster.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "food_water_mask.tif",
        },
        {
            'expression': 'raster1>0.9',
            'symbol_to_path_map': { 
                'raster1': r"C:\Users\Becky\Documents\cnc_project\output_CBD\food_hazards_average_raster.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "food_hazards_mask.tif",
        },
        {
            'expression': 'raster1>0.9',
            'symbol_to_path_map': { 
                'raster1': r"C:\Users\Becky\Documents\cnc_project\output_CBD\hazards_water_average_raster.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "hazards_water_mask.tif",
        },
        {
            'expression': 'raster1>0.9',
            'symbol_to_path_map': { 
                'raster1': r"C:\Users\Becky\Documents\cnc_project\output_CBD\food_water_hazards_average_raster.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "food_water_hazards_mask.tif",
        },
    ]

    for calculation in raster_calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)    

    single_expression = {
        'expression': '(raster2>-9999)*raster1',
        'symbol_to_path_map': {
            'raster1': r"solution_111_tar_80_res_2km_carbon_0.tif",
            'raster2': r"realized_e_source_abs_ann_mean.tif"
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_projection_wkt': wgs84_srs.ExportToWkt(),
        'target_pixel_size': (1.495833333333333348,1.5092592592592593),
        'resample_method': 'average',
        'target_raster_path': "top80_solution_1.5d_avg.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)    

    single_expression = {
        'expression': '(raster2>-9999)*raster1',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Dropbox\NatCap\projects\CI-CNC\Final figs\Fig1_green_blue\cntr_2km_nocarb_land2.tif",
            'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\realized_e_source_abs_ann_mean.tif"
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_sr_wkt': wgs84_srs.ExportToWkt(),
        'target_pixel_size': (1.495833333333333348,1.5092592592592593),
        'resample_method': 'mode',
        'target_raster_path': "cntr_2km_nocarb_land_resampled15_mode.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': '(raster1>2)*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\prioritiz-2km-country\cntr_2km_nocarb.tif",
            'raster2': r"C:\Users\Becky\Documents\cnc_project\Total_C_v10_2km_optimization_output_2020_08_18\optimal_mask_0.65.tif"
        },
        'target_nodata': -9999,
        'target_pixel_size': (0.021319, 0.021319),
        'resample_method': 'average',
        'target_raster_path': "ctr90_C65_2km.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return
    
    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)    

    single_expression = {
        'expression': '(raster2>-9999)*raster1',
        'symbol_to_path_map': {
            #'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\output_2km_masks\solution_111_tar_90_res_2km_carbon_0.tif",
            'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\output_2km_masks\solution_222_tar_90_res_2km.tif",
            'raster2': r"C:\Users\Becky\Documents\cnc_project\original_rasters\realized_e_source_abs_ann_mean.tif"
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_sr_wkt': wgs84_srs.ExportToWkt(),
        'target_pixel_size': (1.495833333333333348,1.5092592592592593),
        'resample_method': 'mode',
        #'target_raster_path': "solution_111_tar_90_res_2km_carbon_0_resampled15_mode.tif",
        'target_raster_path': "solution_222_tar_90_res_2km_resampled15_mode.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': '(raster1>2)*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\prioritiz-2km-country\cntr_2km_nocarb.tif",
            'raster2': r"C:\Users\Becky\Documents\cnc_project\Total_C_v10_2km_optimization_output_2020_08_18\optimal_mask_0.65.tif"
        },
        'target_nodata': -9999,
        'target_pixel_size': (0.021319, 0.021319),
        'resample_method': 'average',
        'target_raster_path': "ctr90_C65_2km.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': '(raster1>=0)*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\optimization\prioritiz-2km-country\cntr_2km_nocarb.tif",
            'raster2': r"C:\Users\Becky\Documents\raster_calculations\Total_C_v10_300m.tif"
        },
        'target_nodata': -9999,
        'target_pixel_size': (0.021319, 0.021319),
        'resample_method': 'average',
        'target_raster_path': "Total_C_v10_2km.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    

    NNth_poll = 38
    Max_lang = 43 #original map did not exceed 43 languages per degree grid cell; higher than that must be an error
    LO = 0.001 # not contributing much below this point!
    LOth_ffish = 0.001 # Min values are regression artifacts. Should be cut off at 10-1 tons per grid cell (~100 sq km). That’s 1 kg per sq km
    NNth_ffish = 30 # Max cut-off should be 3000 tons per grid cell. That’s 30 tons per sq km. (In between the 99 and 99.9th percentiles once small values are excluded)
    #Max_mfish = 400 #this one's different because even though it's higher than the 99th percentile, there are some realistic values of up to 346 kg /km2
    #NOTE: Rachel subsequently asked Reg Watson about this and he said it should NOT be clamped - if anything his upper values (of a few thousand) are underestimates
    LOth_MM = 0.001

    clamped_service_list = [ #some services just have crazy high values that throw the whole percentiles off so we're clamping them to the 99th percentile
        {
            'expression': f'(service>{NNth_poll})*({NNth_poll})+(service<={NNth_poll})*(service>={LO})*service + 0*(service<{LO})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\resampled_30s\realized_pollination_nathab30s.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_pollination_nathab30s_clamped.tif",
        },
        {
            'expression': f'(service>{Max_lang})*(128) + (service<={Max_lang})*service', 
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\resampled_30s\realized_cultural_language_nathab30s.tif",
            },
            'target_nodata': 128,
            'target_raster_path': "realized_cultural_language_nathab30s_clamped.tif",
        },
        {
            'expression': f'(service>{NNth_ffish})*{NNth_ffish} + (service<={NNth_ffish})*(service>={LOth_ffish})*service + 0*(service<{LOth_ffish})', 
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\resampled_30s\realized_fwfish_nathab30s.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_fwfish_per_km2_30s_clamped.tif",
        },
    #    {
    #        'expression': f'(service>{Max_mfish})*({Max_mfish})+(service<={Max_mfish})*(service>={LO})*service+ 0*(service<{LO})', 
    #        'symbol_to_path_map': {
    #            'service': r"C:\Users\Becky\Documents\cnc_project\resampled_30s\realized_marinefish_watson_2010_2014_30s.tif",
    #        },
    #        'target_nodata': -9999,
    #        'target_raster_path': "realized_marinefish_watson_2010_2014_30s_clamped.tif",
    #    },
        {
            'expression': f'(service>{LOth_MM})*service + 0*(service<={LOth_MM})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\resampled_30s\realized_commercialtimber_forest30s.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_commercialtimber_forest30s_clamped.tif",
        },
        {
            'expression': f'(service>{LOth_MM})*service + 0*(service<={LOth_MM})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\resampled_30s\realized_domestictimber_forest30s.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_domestictimber_forest30s_clamped.tif",
        },
        {
            'expression': f'(service>{LOth_MM})*service + 0*(service<={LOth_MM})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\resampled_30s\realized_flood_nathab30s.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_flood_nathab30s_clamped.tif",
        },
        {
            'expression': f'(service>{LOth_MM})*service + 0*(service<={LOth_MM})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\resampled_30s\realized_fuelwood_forestshrub30s.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_fuelwood_forest30s_clamped.tif",
        },
        {
            'expression': f'(service>{LOth_MM})*service + 0*(service<={LOth_MM})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\resampled_30s\realized_grazing_natnotforest30s.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_grazing_natnotforest30s_clamped.tif",
        },
        {
            'expression': f'(service>{LO})*service + 0*(service<={LO})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\resampled_30s\realized_natureaccess10_nathab30s.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_natureaccess10_nathab30s_clamped.tif",
        },
        {
            'expression': f'(service>{LO})*service + 0*(service<={LO})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\resampled_30s\realized_natureaccess100_nathab30s.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_natureaccess100_nathab30s_clamped.tif",
        },
    ]

    for calculation in clamped_service_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    masked_service_list = [
        {
            'expression': 'service*mask + 128*(1-mask)', #this sets all values not in the mask to nodata (in this case, 128)
            'symbol_to_path_map': {
                'mask': r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_esa2015_30s.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\half_degree_grid_langa_19_dslv_density.tif",
            },
            'target_nodata': 128,
            'target_raster_path': "realized_cultural_language_nathab30s.tif",
            'target_pixel_size': (0.008333333333333333218, -0.008333333333333333218),
            'resample_method': 'average',
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_esa2015_30s.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\realized_sedimentdeposition_nathab_clamped_md5_30d4d6ac5ff4bca4b91a3a462ce05bfe.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_sedimentdeposition_nathab30s.tif",
            'target_pixel_size': (0.008333333333333333218, -0.008333333333333333218),
            'resample_method': 'average',
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_esa2015_30s.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\realized_pollination_md5_443522f6688011fd561297e9a556629b.tif"
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_pollination_nathab30s.tif",
            'target_pixel_size': (0.008333333333333333218, -0.008333333333333333218),
            'resample_method': 'average',
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_esa2015_30s.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\realized_nitrogenretention_downstream3s_10s.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_nitrogenretention_nathab30s.tif",
            'target_pixel_size': (0.008333333333333333218, -0.008333333333333333218),
            'resample_method': 'average',
        },
        {
            'expression': '((service<0)*(-9999)+(service>=0)*service)*mask + -9999*(1-mask)', #this both sets all negative values to nodata AND sets anything outside the mask to nodata
            'symbol_to_path_map': {
                'mask': r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_esa2015_30s.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\realized_e_source_ratio_ann_mean.tif",
            },
            'target_nodata': -9999,
            'default_nan': -9999, # this is necessary because there are apparently nans in this list!
            'target_raster_path': "realized_moisturerecycling_nathab30s.tif",
            'target_pixel_size': (0.008333333333333333218, -0.008333333333333333218),
            'resample_method': 'average',
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_esa2015_30s.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\RealInflGStoragePop.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_flood_nathab30s.tif",
            'target_pixel_size': (0.008333333333333333218, -0.008333333333333333218),
            'resample_method': 'average',
        },
    ]

    for calculation in masked_service_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    masked_service_list = [
        {
            'expression': 'service*mask + 128*(1-mask)', #this sets all values not in the mask to nodata (in this case, 128)
            'symbol_to_path_map': {
                'mask': r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_wstreams_esa2015_30s.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\half_degree_grid_langa_19_dslv_density.tif",
            },
            'target_nodata': 128,
            'target_raster_path': "realized_cultural_language_nathab30s.tif",
            'target_pixel_size': (0.008333333333333333218, -0.008333333333333333218),
            'resample_method': 'average',
        },
        {
            'expression': '((service<0)*(-9999)+(service>=0)*service)*mask + -9999*(1-mask)', #this both sets all negative values to nodata AND sets anything outside the mask to nodata
            'symbol_to_path_map': {
                'mask': r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_wstreams_esa2015_30s.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\realized_e_source_ratio_ann_mean.tif",
            },
            'target_nodata': -9999,
            'default_nan': -9999, # this is necessary because there are apparently nans in this list!
            'target_raster_path': "realized_moisturerecycling_nathab30s.tif",
            'target_pixel_size': (0.008333333333333333218, -0.008333333333333333218),
            'resample_method': 'average',
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_wstreams_esa2015_30s.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\RealInflGStoragePop.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_flood_nathab30s.tif",
            'target_pixel_size': (0.008333333333333333218, -0.008333333333333333218),
            'resample_method': 'average',
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_nathab_forest_esa2015_30s.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\realised_commercial_timber_value.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_commercialtimber_forest30s.tif",
            'target_pixel_size': (0.008333333333333333218, -0.008333333333333333218),
            'resample_method': 'average',
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_nathab_forest_esa2015_30s.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\realised_domestic_timber_value.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_domestictimber_forest30s.tif",
            'target_pixel_size': (0.008333333333333333218, -0.008333333333333333218),
            'resample_method': 'average',
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_nathab_forestshrub_esa2015_30s.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\realized_fuelwood.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_fuelwood_forestshrub30s.tif",
            'target_pixel_size': (0.008333333333333333218, -0.008333333333333333218),
            'resample_method': 'average',
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_nathab_notforest_esa2015_30s.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\realized_grazing_md5_19085729ae358e0e8566676c5c7aae72.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_grazing_natnotforest30s.tif",
            'target_pixel_size': (0.008333333333333333218, -0.008333333333333333218),
            'resample_method': 'average',
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_wstreams_esa2015_30s.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\total_pop_near_nature_10.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_natureaccess10_nathab30s.tif",
            'target_pixel_size': (0.008333333333333333218, -0.008333333333333333218),
            'resample_method': 'average',
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_wstreams_esa2015_30s.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\total_pop_near_nature_100.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_natureaccess100_nathab30s.tif",
            'target_pixel_size': (0.008333333333333333218, -0.008333333333333333218),
            'resample_method': 'average',
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_wstreams_esa2015_30s.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\realized_pollination_md5_443522f6688011fd561297e9a556629b.tif"
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_pollination_nathab30s.tif",
            'target_pixel_size': (0.008333333333333333218, -0.008333333333333333218),
            'resample_method': 'average',
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_wstreams_esa2015_30s.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\realized_nitrogenretention_downstream3s_10s.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_nitrogenretention_nathab30s.tif",
            'target_pixel_size': (0.008333333333333333218, -0.008333333333333333218),
            'resample_method': 'average',
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_wstreams_esa2015_30s.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\realized_sedimentdeposition_nathab_clamped_md5_30d4d6ac5ff4bca4b91a3a462ce05bfe.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_sedimentdeposition_nathab30s.tif",
            'target_pixel_size': (0.008333333333333333218, -0.008333333333333333218),
            'resample_method': 'average',
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask': r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_wstreams_esa2015_30s.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\per_km_2_realized_fwfish_distrib_catch_md5_995d3d330ed5fc4462a47f7db44225e9.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_fwfish_nathab30s.tif",
            'target_pixel_size': (0.008333333333333333218, -0.008333333333333333218),
            'resample_method': 'average',
        },
    ]

    for calculation in masked_service_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    single_expression = {
        'expression': 'service*pop',
        'symbol_to_path_map': {
            'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\need_processing\potential_nitrogenretention3s_10s_clamped.tif",
            'pop': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\need_processing\beneficiaries_downstream_nathab_md5_db1311d54c0174c932cc676bbd621643.tif",
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_raster_path': "realized_nitrogenretention_downstream3s_10s.tif",
        'target_pixel_size': (0.002777777777778, -0.002777777777778),
        'resample_method': 'average'
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()

    single_expression = {
        'expression': '(service>=0)*(service<186)*service + (service>=186)*186 + (service<0)*0',
        'symbol_to_path_map': {
            'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\need_processing\potential_nitrogenretention3s_10s.tif",
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_raster_path': "potential_nitrogenretention3s_10s_clamped.tif",
        'target_pixel_size': (0.002777777777778, -0.002777777777778),
        'resample_method': 'average'
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    

    single_expression = {
        'expression': 'load - export',
        'symbol_to_path_map': {
            'load': r"C:\Users\Becky\Documents\modified_load_n_baseline_napp_rate_global_md5_00d3e7f1abc5d6aee99d820cd22ef7da.tif",
            'export': r"C:\Users\Becky\Documents\n_export_baseline_napp_rate_global_md5_b210146a5156422041eb7128c147512f.tif",
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_raster_path': "potential_nitrogenretention3s_10s.tif",
        'target_pixel_size': (0.002777777777778, -0.002777777777778),
        'resample_method': 'average'
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    # resampling of just one raster doesn't work in raster calculations, so just use pygeoprocessing directly
    pygeoprocessing.warp_raster(
        r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_esa2015.tif",
        (30/3600, -30/3600), 'masked_all_nathab_esa2015_30s.tif',
        'mode'
    )

    TASK_GRAPH.join()

    pygeoprocessing.warp_raster(
        r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_wstreams_esa2015.tif",
        (30/3600, -30/3600), 'masked_all_nathab_wstreams_esa2015_30s.tif',
        'mode'
    )

    TASK_GRAPH.join()

    pygeoprocessing.warp_raster(
        r"C:\Users\Becky\Documents\raster_calculations\masked_nathab_forest_esa2015.tif",
        (30/3600, -30/3600), 'masked_nathab_forest_esa2015_30s.tif',
        'mode'
    )

    TASK_GRAPH.join()

    pygeoprocessing.warp_raster(
        r"C:\Users\Becky\Documents\raster_calculations\masked_nathab_forestshrub_esa2015.tif",
        (30/3600, -30/3600), 'masked_nathab_forestshrub_esa2015_30s.tif',
        'mode'
    )

    TASK_GRAPH.join()

    pygeoprocessing.warp_raster(
        r"C:\Users\Becky\Documents\raster_calculations\masked_nathab_notforest_esa2015.tif",
        (30/3600, -30/3600), 'masked_nathab_notforest_esa2015_30s.tif',
        'mode'
    )

    TASK_GRAPH.join()

    #now doing all the layers that don't need to get masked by habitat (because they're already on the habitat or they can't be)
    pygeoprocessing.warp_raster(
        r"C:\Users\Becky\Documents\cnc_project\original_rasters\realized_coastalprotection_md5_b8e0ec0c13892c2bf702c4d2d3e50536.tif",
        (30/3600, -30/3600), 'realized_coastalprotection_30s.tif',
        'average'
    )

    TASK_GRAPH.join()

    pygeoprocessing.warp_raster(
        r"C:\Users\Becky\Documents\cnc_project\original_rasters\watson_2010_2014_catch_per_sqkm_AVG.tif",
        (30/3600, -30/3600), 'realized_marinefish_watson_2010_2014_30s.tif',
        'average'
    )

    TASK_GRAPH.join()

    pygeoprocessing.warp_raster(
        r"C:\Users\Becky\Documents\cnc_project\original_rasters\realized_coastalprotection_barrierreef_md5_126320d42827adc0f7504d4693c67e18.tif",
        (30/3600, -30/3600), 'realized_coastalprotection_barrierreef_30s.tif',
        'average'
    )

    TASK_GRAPH.join()

    #this one's also in a different CRS so needs to be reprojected
    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)

    pygeoprocessing.warp_raster(
        r"C:\Users\Becky\Documents\cnc_project\original_rasters\Modelled_Total_Dollar_Value_of_Reef_Tourism_USD_per_km2.tif",
        (30/3600, -30/3600), 'realized_reeftourism_30s.tif',
        'average', target_sr_wkt=wgs84_srs.ExportToWkt()
    )

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return



    masker_list = [
        {
            # this is for masking out forest from natural habitat, for livestock production
            # this counts the >50% herbaceous / < 50% tree cover category as "not forest"; also includes lichens, mosses  and shrubland which maybe isn't totally edible by cattle either
            'expression': 'mask(raster, %s, invert=False)'%(str([x for x in range(100,154)]+[30]+[40]+[180])[1:-1]),
            'symbol_to_path_map': {
                'raster': r"C:\Users\Becky\Documents\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "masked_nathab_notforest_esa2015.tif",
        },
        {
            'expression': 'mask(raster, %s, invert=False)'%(str([x for x in range(30,111)]+[150]+[151]+[160]+[170])[1:-1]),
            'symbol_to_path_map': {
                'raster': r"C:\Users\Becky\Documents\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "masked_nathab_forest_esa2015.tif",
        },
        {
            'expression': 'mask(raster, %s, invert=False)'%(str([x for x in range(30,123)]+[150]+[151]+[152]+[160]+[170]+[180])[1:-1]),
            'symbol_to_path_map': {
                'raster': r"C:\Users\Becky\Documents\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "masked_nathab_forestshrub_esa2015.tif",
        },
        {
            'expression': 'mask(raster, %s, invert=False)'%(str([]+[x for x in range(30,181)]+[210])[1:-1]),
            'symbol_to_path_map': {
                'raster': r"C:\Users\Becky\Documents\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "masked_all_nathab_wstreams_esa2015.tif",
        },
        {
            'expression': 'mask(raster, %s, invert=False)'%(str([]+[x for x in range(30,181)])[1:-1]),
            'symbol_to_path_map': {
                'raster': r"C:\Users\Becky\Documents\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "masked_all_nathab_esa2015.tif",
        },
    ]
    for masker in masker_list:
       raster_calculations_core.evaluate_calculation(
            masker, TASK_GRAPH, WORKSPACE_DIR)


    TASK_GRAPH.join()
    TASK_GRAPH.close()

#    single_expression = {
#        'expression': '(raster3 > 0) + (raster4 > 0) + (raster5 > 0) + (raster6 > 0) + (raster7 > 0) + (raster11 > 0) + (raster12 > 0) + (raster13 > 0) + (raster15 > 0)',
#        'symbol_to_path_map': {
#            #'raster1': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\realized_coastalprotection_barrierreef_md5_126320d42827adc0f7504d4693c67e18.tif",
#            #'raster2': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\realized_coastalprotection_md5_b8e0ec0c13892c2bf702c4d2d3e50536.tif",
#            'raster3': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\realized_commercialtimber_forest_clamped0_md5_24844213f0f65a6c0bedfebe2fbd089e.tif",
#            'raster4': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\realized_cultural_language_nathab_md5_8e517eaa7db482d1446be5b82152c79b.tif",
#            'raster5': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\realized_domestictimber_forest_clamped0_md5_dca99ceb7dd9f96d54b3fcec656d3180.tif",
#            'raster6': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\realized_flood_nathab_clamped0_md5_eb8fd58621e00c6aeb80f4483da1b35c.tif",
#            'raster7': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\realized_fuelwood_forest_clamped0_md5_4ee236f5400ac400c07642356dd358d1.tif",
#            #'raster8': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\realized_fwfish_per_km2_clamped_1e-3_30_md5_0b4455185988a9e2062a39b27910eb8b.tif",
#            #'raster9': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\realized_grazing_natnotforest_clamped0_md5_8eeb02139f0fabf552658f7641ab7576.tif",
#            #'raster10': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\realized_marinefish_watson_2010_2014_clamped_md5_167448a2c010fb2f20f9727b024efab8.tif",
#            'raster11': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\realized_natureaccess10_nathab_md5_af07e76ecea7fb5be0fa307dc7ff4eed.tif",
#            'raster12': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\realized_nitrogenretention_nathab_clamped_md5_fe63ffd7c6633f336c91241bbd47bddd.tif",
#            'raster13': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\realized_pollination_nathab_clamped_md5_c9486d6c8d55cea16d84ff4e129b005a.tif",
#            #'raster14': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\realized_reeftourism_Modelled_Total_Dollar_Value_md5_171a993b8ff40d0447f343dd014c72e0.tif",
#            'raster15': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\realized_sedimentdeposition_nathab_clamped_md5_30d4d6ac5ff4bca4b91a3a462ce05bfe.tif"
#        },
#        'target_nodata': -9999,
#        'default_nan': -9999,
#        'target_raster_path': "zeroes_in_forest.tif"
#    }
#
#    raster_calculations_core.evaluate_calculation(
#        single_expression, TASK_GRAPH, WORKSPACE_DIR)
#
#    TASK_GRAPH.join()
#    TASK_GRAPH.close()
#
#    return


    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)

    single_expression = {
        'expression': 'mask*raster',
        #'expression': '(service>=0)*(service<101)*service + (service>=101)*101 + (service<0)*0',
        'symbol_to_path_map': {
            'mask': r"C:\Users\Becky\Dropbox\NatCap\projects\NASA GEOBON\data\CR_intersecting_wsheds_26917.tif",
            #'raster': r"C:\Users\Becky\Documents\ESACCI_LC_L4_LCCS_borrelli_sed_export_compressed_md5_19cd746cdeb63bd0ced4815071b252bf.tif",
            #'raster': r"C:\Users\Becky\Documents\n_export_baseline_napp_rate_global_md5_b210146a5156422041eb7128c147512f.tif"
            #'raster': r"C:\Users\Becky\Documents\cnc_project\original_rasters\potential_nitrogenretention3s_10s_clamped.tif"
            #'raster': r"C:\Users\Becky\Documents\cnc_project\original_rasters\potential_sedimentdeposition_md5_aa9ee6050c423b6da37f8c2723d9b513.tif"
            #'service':r"C:\Users\Becky\Documents\raster_calculations\ESA_sed_retention_CR.tif",
            'raster': r"C:\Users\Becky\Documents\cnc_project\original_rasters\cv_service_sum_md5_0f86665de086aba2e16dca68ac859428.tif",
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        #'target_raster_path': "ESA_sed_export_CR.tif",
        #'target_raster_path': "ESA_n_export_CR.tif",
        #'target_raster_path': "ESA_n_retention_CR.tif",
        #'target_raster_path': "ESA_sed_retention_CR.tif",
        'target_raster_path': "ESA_WCMC_coastal_protection_CR.tif",
        'target_sr_wkt': wgs84_srs.ExportToWkt(),
        'target_pixel_size': (0.002777777777778, -0.002777777777778),
        'resample_method': 'average'
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    Max_mfish = 400 #this one's different because even though it's higher than the 99th percentile, there are some realistic values of up to 346 kg /km2
    clamped_service_list = [ #some services just have crazy high values that throw the whole percentiles off so we're clamping them to the 99th percentile
        {
            'expression': f'(service>{Max_mfish})*({Max_mfish})+(service<={Max_mfish})*service',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\watson_2010_2014_catch_per_sqkm_AVG.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_marinefish_watson_2010_2014_clamped.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
    ]

    for calculation in clamped_service_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    single_expression = {
        'expression': 'service * pop',
        'symbol_to_path_map': {
            'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\need_processing\reefs\barrier_reef_service_average_raster_md5_e12c2928e16bdbad45ce4220d18a5889.tif",
            'pop': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\need_processing\reefs\barrier_reef_pop_average_raster_md5_8387777dc970a55e7b5f5949791cf1ef.tif",
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_raster_path': "realized_coastalprotection_barrierreef.tif",
        'target_pixel_size': (0.002777777777778, -0.002777777777778),
        'resample_method': 'average'
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    single_expression = {
        'expression': 'service * pop',
        'symbol_to_path_map': {
            'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\cv_pop_sum_md5_954b755a9300ceb03a284197672b3656.tif",
            'pop': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\cv_service_sum_md5_0f86665de086aba2e16dca68ac859428.tif",
        },
        'target_nodata': -9999,
        'target_raster_path': "realized_coastalprotection.tif",
        'target_pixel_size': (0.002777777777778, -0.002777777777778),
        'resample_method': 'average'
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': 'service*pop',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\potential_nitrogenretention_nathab_clamped_md5_bf6ce40d6d9e8c8c1b2774b375b85b8a.tif",
                'pop':r"C:\Users\Becky\Documents\cnc_project\masked_rasters\beneficiaries_downstream_nathab_md5_db1311d54c0174c932cc676bbd621643.tif"
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_nitrogenretention_nathab_clamped.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'service*pop',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\potential_sedimentdeposition_nathab_clamped_md5_1d826c8885c6479b6307bc345b95d8bf.tif",
                'pop':r"C:\Users\Becky\Documents\cnc_project\masked_rasters\beneficiaries_downstream_nathab_md5_db1311d54c0174c932cc676bbd621643.tif"
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_sedimentdeposition_nathab_clamped.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'service*pop',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\cv_service_sum_bin_raster_md5_bc04cd7112c865fc12f8229ad4757af5.tif",
                'pop':r"C:\Users\Becky\Documents\cnc_project\masked_rasters\cv_pop_sum_bin_raster_md5_27be87e1a0c5a789c82d84122ebf61b8.tif"
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_coastalprotectionbin.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'service * pop / 10',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\binned_services_global\barrier_reef_service_average_raster_md5_e12c2928e16bdbad45ce4220d18a5889_eez__GLOBAL_bin_nodata0_raster_md5_c271e54f1b04174d3e620df344a52bd9.tif",
                'pop': r"C:\Users\Becky\Documents\cnc_project\binned_services_global\barrier_reef_pop_average_raster_md5_8387777dc970a55e7b5f5949791cf1ef_eez__GLOBAL_bin_nodata0_raster_md5_b36485a7d4f837804982e5e9272d34fe.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_coastalprotectionbin_barrierreef.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
            'resample_method': 'average'
        }

    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    masker_list = [
         {
            # the %s is a placeholder for the string we're passing it using this function that lists every number in the range and takes away the [] of the list and turns it into a string
            'expression': 'mask(raster, %s, invert=False)'%(str([]+[x for x in range(50,181)])[1:-1]),
            #'expression': 'mask(raster, %s, invert=False)'%(str([]+[x for x in range(10,200)]+[220])[1:-1]),
            'symbol_to_path_map': {
                'raster': r"C:\Users\Becky\Documents\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "masked_all_nathab_esa2015.tif",
        },
        {
            # this is for masking out forest from natural habitat, for livestock production
            # this counts the >50% herbaceous / < 50% tree cover category as "not forest"; also includes lichens, mosses  and shrubland which maybe isn't totally edible by cattle either
            'expression': 'mask(raster, %s, invert=False)'%(str([x for x in range(110,154)]+[180])[1:-1]),
            'symbol_to_path_map': {
                'raster': r"C:\Users\Becky\Documents\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "masked_nathab_notforest_esa2015.tif",
        },
        {
            'expression': 'mask(raster, %s, invert=False)'%(str([x for x in range(50,111)]+[150]+[151]+[160]+[170])[1:-1]),
            'symbol_to_path_map': {
                'raster': r"C:\Users\Becky\Documents\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "masked_nathab_forest_esa2015.tif",
        },
        {
            'expression': 'mask(raster, %s, invert=False)'%(str([]+[x for x in range(50,181)]+[210])[1:-1]),
            'symbol_to_path_map': {
                'raster': r"C:\Users\Becky\Documents\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "masked_all_nathab_wstreams_esa2015.tif",
        },
        {
           'expression': 'mask(raster, %s, invert=False)'%(str([]+[x for x in range(10,31)])[1:-1]),
            'symbol_to_path_map': {
                'raster': r"C:\Users\Becky\Documents\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "agmask_esa2015.tif",
        },
    ]
    for masker in masker_list:
       raster_calculations_core.evaluate_calculation(
            masker, TASK_GRAPH, WORKSPACE_DIR)


    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    masked_service_list = [
        {
            'expression': 'service*mask + 128*(1-mask)', #this sets all values not in the mask to nodata (in this case, 128)
            'symbol_to_path_map': {
                'mask': r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_esa2015.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\half_degree_grid_langa_19_dslv_density.tif",
            },
            'target_nodata': 128,
            'target_raster_path': "realized_cultural_language_nathab.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': '((service<0)*(-9999)+(service>=0)*service)*mask + -9999*(1-mask)', #this both sets all negative values to nodata AND sets anything outside the mask to nodata
            'symbol_to_path_map': {
                'mask': r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_esa2015.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\realized_e_source_ratio_ann_mean.tif",
            },
            'target_nodata': -9999,
            'default_nan': -9999, # this is necessary because there are apparently nans in this list!
            'target_raster_path': "realized_moisturerecycling_nathab.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_esa2015.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\RealInflGStoragePop.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_flood_nathab.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_nathab_forest_esa2015.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\realised_commercial_timber_value.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_commercialtimber_forest.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_nathab_forest_esa2015.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\realised_domestic_timber_value.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_domestictimber_forest.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_nathab_forest_esa2015.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\realized_fuelwood.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_fuelwood_forest.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_nathab_notforest_esa2015.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\realized_grazing_md5_19085729ae358e0e8566676c5c7aae72.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_grazing_natnotforest.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_esa2015.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\total_pop_near_nature_10.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_natureaccess10_nathab.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_esa2015.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\total_pop_near_nature_100.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_natureaccess100_nathab.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_esa2015.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\potential_nitrogenretention_md5_286c51393042973f71884ddc701be03d.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "potential_nitrogenretention_nathab.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_esa2015.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\potential_sedimentdeposition_md5_aa9ee6050c423b6da37f8c2723d9b513.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "potential_sedimentdeposition_nathab.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_esa2015.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\downstream_beneficiaries_md5_68495f4bbdd889d7aaf9683ce958a4fe.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "beneficiaries_downstream_nathab.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_esa2015.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\original_rasters\realized_pollination_md5_443522f6688011fd561297e9a556629b.tif"
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_pollination_nathab.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_esa2015.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\realized_nitrogenretention_downstream_md5_82d4e57042482eb1b92d03c0d387f501.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_nitrogenretention_nathab.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'service*mask + -9999*(1-mask)',
            'symbol_to_path_map': {
                'mask':  r"C:\Users\Becky\Documents\raster_calculations\masked_all_nathab_esa2015.tif",
                'service': r"C:\Users\Becky\Documents\cnc_project\realized_sedimentdeposition_downstream_md5_1613b12643898c1475c5ec3180836770.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_sedimentdeposition_nathab.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
    ]

    for calculation in masked_service_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    NNth_nit = 322
    NNth_sed = 161
    NNth_poll = 982
    NNth_ffish = 75
    LOth_ffish = 0.001 # Min values are regression artifacts. Should be cut off at 10-1 tons per grid cell (~100 sq km). That’s 1 kg per sq km
    NNth_ffish = 30 # Max cut-off should be 3000 tons per grid cell. That’s 30 tons per sq km. (In between the 99 and 99.9th percentiles once small values are excluded)
    Max_mfish = 400 #this one's different because even though it's higher than the 99th percentile, there are some realistic values of up to 346 kg /km2
    LOth_MM = 0.00001
    clamped_service_list = [ #some services just have crazy high values that throw the whole percentiles off so we're clamping them to the 99th percentile
        {
            'expression': f'(service>{NNth_nit})*{NNth_nit} + (service<={NNth_nit})*(service>=0)*service + -9999*(service<0)', #sets anything above the 99th percentile value to that value, anything negative to nodata
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\needed_clamping\potential_nitrogenretention_nathab_md5_95b25783b6114b63738f8d6b20d2af51.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "potential_nitrogenretention_nathab_clamped.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>{NNth_sed})*({NNth_sed})+(service<={NNth_sed})*(service>=0)*service + -9999*(service<0)',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\needed_clamping\potential_sedimentdeposition_nathab_md5_1a0dd289bee1fe09c30453ab80f9ddf4.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "potential_sedimentdeposition_nathab_clamped.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>{NNth_poll})*({NNth_poll})+(service<={NNth_poll})*(service>=0)*service + -9999*(service<0)',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\needed_clamping\realized_pollination_nathab_md5_feab479b3d6bf25a928c355547c9d9ab.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_pollination_nathab_clamped.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>{NNth_ffish})*{NNth_ffish} + (service<={NNth_ffish})*(service>={LOth_ffish})*service + -9999*(service<{LOth_ffish})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\needed_clamping\per_km_2_realized_fwfish_distrib_catch_md5_995d3d330ed5fc4462a47f7db44225e9.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_fwfish_per_km2_clamped_3e-2_13.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>{Max_mfish})*({Max_mfish})+(service<={Max_mfish})*service',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\needed_clamping\needed_clamping\realized_marinefish_watson_2015_catch_Ind_Non_Ind_Rprt_IUU_md5_61e08ed60006e9ad23b74bcd44c61548.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_marinefish_watson_2015_clamped.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>{LOth_MM})*service + 0*(service<={LOth_MM})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\needed_clamping\realized_commercialtimber_forest_md5_99153e7a8177fd7ed6bb75a5fdc426e5.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_commercialtimber_forest_clamped.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>{LOth_MM})*service + 0*(service<={LOth_MM})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\needed_clamping\realized_domestictimber_forest_md5_3ee8a15ce8ed38b0710b8f6d74640b70.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_domestictimber_forest_clamped.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>{LOth_MM})*service + 0*(service<={LOth_MM})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\needed_clamping\realized_flood_nathab_md5_bf277802945a0a7067d2a90941e355e1.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_flood_nathab_clamped.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>{LOth_MM})*service + 0*(service<={LOth_MM})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\needed_clamping\realized_fuelwood_forest_md5_e86706b0ebe0d296acac30db78f2c284.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_fuelwood_forest_clamped.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>{LOth_MM})*service + 0*(service<={LOth_MM})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\masked_rasters\needed_clamping\realized_grazing_natnotforest_md5_fbc4907814187d1be75b35932617af65.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_grazing_natnotforest_clamped.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
    ]

    for calculation in clamped_service_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    N90 = 7.3
    S90 = 9.8
    P90 = 8.4
    FF90 = 9.5
    MF90 =  9.3
    CT90 = 4.2
    DT90 = 5.8
    FW90 = 6.2
    F90 = 7.9
    G90 = 4.9
    CL90 = 6.1
    MR90 = 4.4
    NA90 = 8.2
    RT90 = 3.9
    CP90 = 2.7

    top_values_list = [
        {
            'expression': f'(service>={N90}) + 0*(service<{N90})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\binned_services_global\realized_nitrogenretention_bin.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_nitrogenretention_nathab_top90.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>={S90}) + 0*(service<{S90})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\binned_services_global\realized_sedimentdeposition_bin.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_sedimentdeposition_nathab_top90.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>={P90}) + 0*(service<{P90})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\binned_services_global\realized_pollination_bin.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_pollination_nathab_top90.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>={FF90}) + 0*(service<{FF90})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\binned_services_global\realized_fwfish_bin.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_fwfish_per_km2_top90_3e-2_13.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>={MF90}) + 0*(service<{MF90})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\binned_services_global\realized_marinefish_bin.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_marinefish_watson_2015_top90.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>={CT90}) + 0*(service<{CT90})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\binned_services_global\realized_commercialtimber_bin.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_commercialtimber_forest_top90.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>={DT90}) + 0*(service<{DT90})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\binned_services_global\realized_domestictimber_binf.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_domestictimber_forest_top90.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>={F90}) + 0*(service<{F90})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\binned_services_global\realized_flood_bin.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_flood_nathab_top90.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>={FW90}) + 0*(service<{FW90})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\binned_services_global\realized_fuelwood_bin.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_fuelwood_forest_top90.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>={G90}) + 0*(service<{G90})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\binned_services_global\realized_grazing_bin.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_grazing_natnotforest_top90.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>={CL90}) + 0*(service<{CL90})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\binned_services_global\realized_cultural_language_bin.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_cultural_language_nathab_top90.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>={MR90}) + 0*(service<{MR90})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\binned_services_global\realized_moisturerecycling_bin.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_moisturerecycling_nathab_top90.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>={NA90}) + 0*(service<{NA90})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\binned_services_global\realized_natureaccess10_bin.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_natureaccess10_nathab_top90.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>={RT90}) + 0*(service<{RT90})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\binned_services_global\realized_reeftourism_bin.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_reeftourism_top90.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': f'(service>={CP90}) + 0*(service<{CP90})',
            'symbol_to_path_map': {
                'service': r"C:\Users\Becky\Documents\cnc_project\binned_services_global\realized_coastalprotectionbin_plusbarrierreefs_md5_a3f43a2e60e5976799d257ad9561731f.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "realized_coastalprotection_top90.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
    ]

    for calculation in top_values_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    #loop to set thresholds

    for base_raster_path, threshold, target_raster_path in [
            #(r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\normalized_realized_pollination_md5_06f52f2854ae1c584742d587b1c31359.tif", 0.06, "top04_pollination.tif"),
            #(r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\normalized_realized_flood_md5_f1237e76a41039e22629abb85963ba16.tif", 0.05, "top30_flood.tif"),
            #(r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\normalized_realized_grazing_md5_d03b584dac965539a77bf96cba3f8096_masked_md5_db038b499342efa926c3c5815c822fe3.tif", 0.1, "top15_grazing.tif"),
            #(r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\normalized_realized_nitrogen_downstream_md5_437e1759b0f994b47add4baf76509bbe_masked_md5_ac82368cedcfc692b0440b0cc0ed7fdb.tif", 0.06, "top25_nitrogen.tif"),
            #(r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\normalized_realized_nwfp_masked_md5_754ba4d8cd0c54399fd816748a9e0091_masked_md5_f48ada73cb74cd59726b066db2f03855.tif", 0.05, "top10_nwfp.tif"),
            #(r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\normalized_realized_sediment_downstream_md5_daa86f70232c5e1a8a0efaf0b2653db2_masked_md5_6e9050a9fcf3f08925343a48208aeab8.tif", 0.09, "top05_sediment.tif"),
            #(r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\normalized_realized_timber_masked_md5_fc5ad0ff1f4702d75f204267fc90b33f_masked_md5_68df861a8e4c5cbb0e800f389690a792.tif", 0.13, "top15_timber.tif"),
            (r"C:\Users\Becky\Documents\raster_calculations\aggregate_realized_ES_score_nspwogf_md5_0ab07f38ed0290fea6142db188ae51f8.tif", 0.30, "top40_nspwogf.tif"),
            ]:

        mask_expr_dict = {
            'expression': 'raster > %f' % threshold,
            'symbol_to_path_map': {
                'raster': base_raster_path,
            },
            'target_nodata': -1,
            'target_raster_path': target_raster_path,
        }

        raster_calculations_core.evaluate_calculation(
            mask_expr_dict, TASK_GRAPH, WORKSPACE_DIR)


    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    #looping the same mask over a bunch of rasters

    base_directory = r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace"

    masked_workspace_dir = 'masked_workspace_dir'
    ecoshard_workspace_dir = 'ecoshard_dir'
    for dirname in [masked_workspace_dir, ecoshard_workspace_dir]:
        try:
            os.makedirs(dirname)
        except OSError:
            pass

    for path in glob.glob(os.path.join(base_directory, '*.tif')):
        path_root_name = os.path.splitext(os.path.basename(path))[0]
        target_raster_path = os.path.join(
            masked_workspace_dir, '%s_masked.tif' % (path_root_name))

        remasking_expression = {
                'expression': 'mask*service',
                'symbol_to_path_map': {
                    'mask': 'masked_nathab_esa_nodata_md5_7c9acfe052cb7bdad319f011e9389fb1.tif',
                    'service': path,
                },
                'target_nodata': -1,
                'target_raster_path': target_raster_path,
                 ###file name split off from its path and its ecoshard too because it will be re-ecosharded
                'target_pixel_size': (0.002777777777778, -0.002777777777778),
            }

        raster_calculations_core.evaluate_calculation(
            remasking_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    subprocess.check_call("python -m ecoshard ./masked_workspace_dir/*.tif --hash_file --rename --buildoverviews --interpolation_method average")

    TASK_GRAPH.join()
    TASK_GRAPH.close()



    clamping_service_list = [
        {
            'expression': '(val >= 0) * (val < 1) * val + (val >= 1)',
            'symbol_to_path_map': {
                'val': "raw_normalized_potential_flood.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "normalized_potential_flood.tif",
        },
        {
            'expression': '(val >= 0) * (val < 1) * val + (val >= 1)',
            'symbol_to_path_map': {
                'val': "raw_normalized_potential_moisture.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "normalized_potential_moisture.tif",
        },
        {
            'expression': '(val >= 0) * (val < 1) * val + (val >= 1)',
            'symbol_to_path_map': {
                'val': "raw_normalized_realized_flood.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "normalized_realized_flood.tif",
        },
        {
            'expression': '(val >= 0) * (val < 1) * val + (val >= 1)',
            'symbol_to_path_map': {
                'val': "raw_normalized_realized_moisture.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "normalized_realized_moisture.tif",
        },
    ]

    for calculation in clamping_service_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)


    TASK_GRAPH.join()
    TASK_GRAPH.close()


    return #terminates at this point


    # just build overviews
    raster_calculation_list = [
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': '../nathab_potential_pollination.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "potential_pollination.tif",
            'build_overview': True,
        },
    ]

    for calculation in raster_calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()



     #dasgupta calcs:

    raster_list = [
        {
            'expression': 'total_realized /total_potential',
            'symbol_to_path_map': {
                'total_realized': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4\prod_total_realized_en_10s_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
                'total_potential': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4\prod_total_potential_en_10s_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
            },
            'target_nodata': -1,
            'default_nan': -1, # this is necessary because divides by 0's; could also set them to 0 instead
            'target_raster_path': "percent_realized_current.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'total_realized /total_potential',
            'symbol_to_path_map': {
                'total_realized': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_bau_esa_classes_md5_b411f14d7cff237e3415c5afa26d4b78\prod_total_realized_en_10s_lulc_WB_bau_esa_classes_md5_b411f14d7cff237e3415c5afa26d4b78.tif",
                'total_potential': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_bau_esa_classes_md5_b411f14d7cff237e3415c5afa26d4b78\prod_total_potential_en_10s_lulc_WB_bau_esa_classes_md5_b411f14d7cff237e3415c5afa26d4b78.tif",
            },
            'target_nodata': -1,
            'default_nan': -1,
            'target_raster_path': "percent_realized_bau.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'total_realized /total_potential',
            'symbol_to_path_map': {
                'total_realized': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_cons_esa_classes_md5_8c150474406a3f230b992399429bd182\prod_total_realized_en_10s_lulc_WB_cons_esa_classes_md5_8c150474406a3f230b992399429bd182.tif",
                'total_potential': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_cons_esa_classes_md5_8c150474406a3f230b992399429bd182\prod_total_potential_en_10s_lulc_WB_cons_esa_classes_md5_8c150474406a3f230b992399429bd182.tif",
            },
            'target_nodata': -1,
            'default_nan': -1,
            'target_raster_path': "percent_realized_cons.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'total_realized /total_potential',
            'symbol_to_path_map': {
                'total_realized': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_mid\prod_total_realized_en_10s_lulc_WB_mid.tif",
                'total_potential': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_mid\prod_total_potential_en_10s_lulc_WB_mid.tif",
            },
            'target_nodata': -1,
            'default_nan': -1,
            'target_raster_path': "percent_realized_mid.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'total_realized /total_potential',
            'symbol_to_path_map': {
                'total_realized': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4\prod_total_realized_va_10s_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
                'total_potential': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4\prod_total_potential_va_10s_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
            },
            'target_nodata': -1,
            'default_nan': -1,
            'target_raster_path': "percent_realized_current_va.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'total_realized /total_potential',
            'symbol_to_path_map': {
                'total_realized': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_bau_esa_classes_md5_b411f14d7cff237e3415c5afa26d4b78\prod_total_realized_va_10s_lulc_WB_bau_esa_classes_md5_b411f14d7cff237e3415c5afa26d4b78.tif",
                'total_potential': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_bau_esa_classes_md5_b411f14d7cff237e3415c5afa26d4b78\prod_total_potential_va_10s_lulc_WB_bau_esa_classes_md5_b411f14d7cff237e3415c5afa26d4b78.tif",
            },
            'target_nodata': -1,
            'default_nan': -1,
            'target_raster_path': "percent_realized_bau_va.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'total_realized /total_potential',
            'symbol_to_path_map': {
                'total_realized': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_cons_esa_classes_md5_8c150474406a3f230b992399429bd182\prod_total_realized_va_10s_lulc_WB_cons_esa_classes_md5_8c150474406a3f230b992399429bd182.tif",
                'total_potential': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_cons_esa_classes_md5_8c150474406a3f230b992399429bd182\prod_total_potential_va_10s_lulc_WB_cons_esa_classes_md5_8c150474406a3f230b992399429bd182.tif",
            },
            'target_nodata': -1,
            'default_nan': -1,
            'target_raster_path': "percent_realized_cons_va.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'total_realized /total_potential',
            'symbol_to_path_map': {
                'total_realized': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_mid\prod_total_realized_va_10s_lulc_WB_mid.tif",
                'total_potential': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_mid\prod_total_potential_va_10s_lulc_WB_mid.tif",
            },
            'target_nodata': -1,
            'default_nan': -1,
            'target_raster_path': "percent_realized_mid_va.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
    ]

    for calculation in raster_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    #NCI

    single_expression = {
        'expression': 'averageraster*mask - raster2*(mask>1)',
        'symbol_to_path_map': {
            'raster2': r"C:\Users\Becky\Documents\raster_calculations\fertilizers\NitrogenApplication_Rate_md5_caee837fa0e881be0c36c1eba1dea44e.tif",
            'averageraster': r"C:\Users\Becky\Documents\raster_calculations\fertilizer_average_raster.tif",
            'mask': r"C:\Users\Becky\Documents\raster_calculations\fertilizer_valid_count_raster.tif",
        },
        'target_nodata': -9999,
        'target_raster_path': "Intensified_NitrogenApplication_Rate_gapfilled.tif",
        'target_pixel_size': (0.08333333333333332871, -0.08333333333333332871),
        'resample_method': 'average'
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return



if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(WORKSPACE_DIR, NCPUS, 5.0)
    main()
