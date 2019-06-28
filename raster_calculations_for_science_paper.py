"""These calculations are for the science paper."""
import sys
import os
import logging
import multiprocessing

import raster_calculations_core
from osgeo import gdal
import taskgraph


WORKSPACE_DIR = 'raster_expression_workspace'
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

gdal.SetCacheMax(2**30)


def main():
    """Write your expression here."""

    raster_calulation_list1 = [
        {
            'expression': 'mask(raster, 0, invert=True)',
            'symbol_to_path_map': {
                'raster': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/relevant_min_pop_cur_md5_8e5ad3e452d021550b1db60f7c2b8b2f.tif',
            },
            'target_nodata': -1,
            'target_raster_path': 'masked_poll_pop_cur.tif',
        },
        {
            'expression': 'mask(raster, 0, invert=True)',
            'symbol_to_path_map': {
                'raster': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/relevant_min_pop_ssp1_md5_eb096b06844053242b868fca0217f5b5.tif',
            },
            'target_nodata': -1,
            'target_raster_path': 'masked_poll_pop_ssp1.tif',
        },
        {
            'expression': 'mask(raster, 0, invert=True)',
            'symbol_to_path_map': {
                'raster': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/relevant_min_pop_ssp3_md5_c332e4c3e7bd612c69f08393020a75da.tif',
            },
            'target_nodata': -1,
            'target_raster_path': 'masked_poll_pop_ssp3.tif',
        },
        {
            'expression': 'mask(raster, 0, invert=True)',
            'symbol_to_path_map': {
                'raster': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/relevant_min_pop_ssp5_md5_67bd95ab4e75622a47284b9da271c4cd.tif',
            },
            'target_nodata': -1,
            'target_raster_path': 'masked_poll_pop_ssp5.tif',
        },
        {
            'expression': 'future/current',
            'symbol_to_path_map': {
                'future': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/ssp1_2050_md5_cf75de1bd71035236594d2e48c31c245.tif',
                'current': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/ssp1_2010_md5_5edda6266351ccc7dbd587c89fa2ab65.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "pop_change_30s_ssp1.tif",
            'build_overview': False,
            'target_pixel_size': (0.125, -0.125),
        },
        {
            'expression': 'future/current',
            'symbol_to_path_map': {
                'future': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/ssp3_2050_md5_b0608d53870b9a7e315bf9593c43be86.tif',
                'current': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/ssp1_2010_md5_5edda6266351ccc7dbd587c89fa2ab65.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "pop_change_30s_ssp3.tif",
            'build_overview': False,
            'target_pixel_size': (0.125, -0.125),
        },
        {
            'expression': 'future/current',
            'symbol_to_path_map': {
                'future': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/ssp5_2050_md5_99dcce1d1578fa9f3906227c112837e5.tif',
                'current': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/ssp1_2010_md5_5edda6266351ccc7dbd587c89fa2ab65.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "pop_change_30s_ssp5.tif",
            'build_overview': False,
            'target_pixel_size': (0.125, -0.125),
        },
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/gpw-v4-population-count_2015_md5_683d55e67b3829ab2d8045ebed9ec5d1_md5_683d55e67b3829ab2d8045ebed9ec5d1.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "pop_30s_cur.tif",
            'build_overview': False,
            'target_pixel_size': (0.00833333333333339, -0.00833333333333339)
        },

        
    ]
    for calculation in  raster_calulation_list1:
       raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)
   
    TASK_GRAPH.join()
    
    raster_calculation_list2 = [
        {
            'expression': 'masker*pop',
            'symbol_to_path_map': {
                'masker': 'masked_poll_pop_cur.tif',
                'pop': 'pop_30s_cur.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "pollination_pop_30s_cur.tif",
            'build_overview': True,
            'target_pixel_size': (0.00833333333333339, -0.00833333333333339)
        },
        {
            'expression': 'change*pop',
            'symbol_to_path_map': {
                'change': 'pop_change_30s_ssp1.tif',
                'pop': 'pop_30s_cur.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "pop_30s_ssp1.tif",
            'build_overview': False,
            'target_pixel_size': (0.00833333333333339, -0.00833333333333339),
        },
        {
            'expression': 'change*pop',
            'symbol_to_path_map': {
                'change': 'pop_change_30s_ssp3.tif',
                'pop': 'pop_30s_cur.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "pop_30s_ssp3.tif",
            'build_overview': False,
            'target_pixel_size': (0.00833333333333339, -0.00833333333333339),
        },
        {
            'expression': 'change*pop',
            'symbol_to_path_map': {
                'change': 'pop_change_30s_ssp5.tif',
                'pop': 'pop_30s_cur.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "pop_30s_ssp5.tif",
            'build_overview': False,
            'target_pixel_size': (0.00833333333333339, -0.00833333333333339),
        }, 
    ]


    for calculation in raster_calculation_list2:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()

    raster_calculation_list3 = [
         {
            'expression': 'masker*pop',
            'symbol_to_path_map': {
                'masker': 'masked_poll_pop_ssp1.tif',
                'pop': 'pop_30s_ssp1.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "pollination_pop_30s_ssp1.tif",
            'build_overview': True,
            'target_pixel_size': (0.00833333333333339, -0.00833333333333339),
        },
        {
            'expression': 'masker*pop',
            'symbol_to_path_map': {
                'masker': 'masked_poll_pop_ssp3.tif',
                'pop': 'pop_30s_ssp3.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "pollination_pop_30s_ssp3.tif",
            'build_overview': True,
            'target_pixel_size': (0.00833333333333339, -0.00833333333333339),
        },
        {
            'expression': 'masker*pop',
            'symbol_to_path_map': {
                'masker': 'masked_poll_pop_ssp5.tif',
                'pop': 'pop_30s_ssp5.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "pollination_pop_30s_ssp5.tif",
            'build_overview': True,
            'target_pixel_size': (0.00833333333333339, -0.00833333333333339),
        },
    ]

    for calculation in raster_calculation_list3:
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
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2015_n_export_compressed_md5_fa15687cc4d4fdc5e7a6351200873578.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/wqr_deficit_cur.tif",
            'build_overview': True,
        },
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/deficit_change_nutrient_10s_ssp1_md5_98fba1a6dbbe52ba322e59a58fdc0a58.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/wqr_deficit_change_s1.tif",
            'build_overview': True,
        },
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/deficit_change_nutrient_10s_ssp3_md5_80c5c4763d663490cfdf1bd4ed602403.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/wqr_deficit_change_s3.tif",
            'build_overview': True,
        },
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/deficit_change_nutrient_10s_ssp5_md5_5c6f5b38753d092fa357d466a89ed63b.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/wqr_deficit_change_s5.tif",
            'build_overview': True,
        },
        {
        'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/NC_nutrient_10s_cur_md5_69bc709bf415377a7e0e527c1dbe88b0.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/wqr_nc_cur.tif",
            'build_overview': True,
        },
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/NCchange_nutrient_10s_ssp1_md5_049a3c8e8d4f6d8795dde81f39cb338a.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/wqr_nc_change_s1.tif",
            'build_overview': True,
        },
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/NCchange_nutrient_10s_ssp3_md5_e5e187f9a100747f7fe1c97b437b95c4.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/wqr_nc_change_s3.tif",
            'build_overview': True,
        },
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/NCchange_nutrient_10s_ssp5_md5_6c8a2ccd47bf397d8b7c2f0f43dedd7e.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/wqr_nc_change_s5.tif",
            'build_overview': True,
        },
        {
        'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/water_ruralpop_30s_2015_md5_2fbcc65a4b24bc4fe3c2d6cfc263fe63.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/wqr_pop_cur.tif",
            'build_overview': True,
        },
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/pop_change_nutrient_30s_ssp1_md5_7898ea0bd64e38f1daab90f89244856c.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/wqr_pop_change_s1.tif",
            'build_overview': True,
        },
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/pop_change_nutrient_30s_ssp3_md5_0d5431333a42b350fe4940acdb452c35.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/wqr_pop_change_s3.tif",
            'build_overview': True,
        },
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/pop_change_nutrient_30s_ssp5_md5_e6efa6245d1c762ebe1a625c2446e712.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/wqr_pop_change_s5.tif",
            'build_overview': True,
        },
        {
        'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2015_modified_load_compressed_md5_e3072705a87b0db90e7620abbc0d75f1.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/wqr_potential_cur.tif",
            'build_overview': True,
        },
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/potential_change_nutrient_10s_ssp1_md5_e4ca2a294d210a296190553b95f32feb.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/wqr_potential_change_s1.tif",
            'build_overview': True,
        },
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/potential_change_nutrient_10s_ssp3_md5_a155fc1e2356dae0b66140d3b9002a2f.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/wqr_potential_change_s3.tif",
            'build_overview': True,
        },
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/potential_change_nutrient_10s_ssp5_md5_48bf7884b115df5cb44bea1f3a434182.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/wqr_potential_change_s5.tif",
            'build_overview': True,
        },
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/deficit_pollination_10s_cur_md5_0e86d02e9a0c96412e5217cee8540821.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/poll_deficit_cur.tif",
            'build_overview': True,
        },
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/deficit_change_pollination_10s_ssp3_md5_e44b21621e077086e7deb2e2fad93062.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/poll_deficit_change_s3.tif",
            'build_overview': True,
        },
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/NC_pollination_10s_cur_md5_c79490ecf640a9e5dc1c5ff1058f340a.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/poll_NC_cur.tif",
            'build_overview': True,
        },
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/NCchange_pollination_10s_ssp3_md5_fcc9ff3d88a392d6fc86cab80c4fa4bc.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/poll_NC_change_s3.tif",
            'build_overview': True,
        },
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/potential_pollination_10s_cur_md5_cd977f88b4408f8d7eacd791c95d7792.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/poll_potential_cur.tif",
            'build_overview': True,
        },
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/potential_change_pollination_10s_ssp3_md5_02df500ba25428ace0d5e92bd2163650.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "science_outputs/poll_potential_change_s3.tif",
            'build_overview': True,
        },
    ]

    for calculation in raster_calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)
   
    TASK_GRAPH.join()
    TASK_GRAPH.close()

    raster_calculation_list = [
        {
            'expression': '(load-export)/load',
            'symbol_to_path_map': {
                'load': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2015_modified_load_compressed_md5_e3072705a87b0db90e7620abbc0d75f1.tif',
                'export': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2015_n_export_compressed_md5_fa15687cc4d4fdc5e7a6351200873578.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/NC_nutrient_10s_cur.tif",
            'build_overview': True,
        },
        {
            'expression': '(load-export)/load',
            'symbol_to_path_map': {
                'load': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2050_ssp1_modified_load_compressed_md5_a5f1db75882a207636546af94cde6549.tif',
                'export': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2050_ssp1_n_export_compressed_md5_4b2b0a4ac6575fde5aca00de4f788494.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/NC_nutrient_10s_ssp1.tif",
            'build_overview': True,
        },
        {
            'expression': '(load-export)/load',
            'symbol_to_path_map': {
                'load': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2050_ssp3_modified_load_compressed_md5_e49e578ed025c0bc796e55b7f27f82f1.tif',
                'export': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2050_ssp3_n_export_compressed_md5_b5259ac0326b0dcef8a34f2086e8339b.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/NC_nutrient_10s_ssp3.tif",
            'build_overview': True,
        },
        {
            'expression': '(load-export)/load',
            'symbol_to_path_map': {
                'load': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2050_ssp5_modified_load_compressed_md5_7337576433238f70140be9ec5b588fd1.tif',
                'export': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2050_ssp5_n_export_compressed_md5_12b9caecc29058d39748e13bf5b5f150.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/NC_nutrient_10s_ssp5.tif",
            'build_overview': True,
        },
        {
            'expression': '(va/486980 + en/3319921 + fo/132654) / 3',
            'symbol_to_path_map': {
                'va': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_potential_va_10s_cur_md5_8e327c260369864d5a38e03279574fb2.tif',
                'en': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_potential_en_10s_cur_md5_a33bd27cb092807455812b6474b88ea3.tif',
                'fo': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_potential_fo_10s_cur_md5_f0660f3e3123ed1b64a502046e4246bd.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/potential_pollination_10s_cur.tif",
            'build_overview': True,
        },
        {
            'expression': '(va/486980 + en/3319921 + fo/132654) / 3',
            'symbol_to_path_map': {
                'va': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_potential_va_10s_ssp1_md5_dd661fc2b46dcaae0291dc8b095162af.tif',
                'en': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_potential_en_10s_ssp1_md5_e38c0f651fd99cc5823c4d4609f3605a.tif',
                'fo': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_potential_fo_10s_ssp1_md5_259247bc5e53dfa4e299f84fcdd970f0.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/potential_pollination_10s_ssp1.tif",
            'build_overview': True,
        },
        {
            'expression': '(va/486980 + en/3319921 + fo/132654) / 3',
            'symbol_to_path_map': {
                'va': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_potential_va_10s_ssp3_md5_9d199ecc7cae7875246fb6c417d36c25.tif',
                'en': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_potential_en_10s_ssp3_md5_c5a582a699913836740b4d8eebff44cc.tif',
                'fo': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_potential_fo_10s_ssp3_md5_8ebf271cbdcd53561b0457de9dc14ff7.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/potential_pollination_10s_ssp3.tif",
            'build_overview': True,
        },
        {
            'expression': '(va/486980 + en/3319921 + fo/132654) / 3',
            'symbol_to_path_map': {
                'va': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_potential_va_10s_ssp5_md5_96374887d44c5f2bd02f1a59bc04081b.tif',
                'en': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_potential_en_10s_ssp5_md5_e97f7cd3bb6d92944f234596718cb9c9.tif',
                'fo': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_potential_fo_10s_ssp5_md5_15dc8849799d0413ab01a842860515cc.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/potential_pollination_10s_ssp5.tif",
            'build_overview': True,
        },
        {
            'expression': '(va/486980 + en/3319921 + fo/132654) / 3',
            'symbol_to_path_map': {
                'va': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_unrealized_va_10s_cur_md5_c8035666f5a6e5c32fb290df989183e2.tif',
                'en': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_unrealized_en_10s_cur_md5_d3e8bc025523d74cd4258f9f954b3cf4.tif',
                'fo': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_unrealized_fo_10s_cur_md5_857aa9c09357ad6614e33f23710ea380.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/deficit_pollination_10s_cur.tif",
            'build_overview': True,
        },
        {
            'expression': '(va/486980 + en/3319921 + fo/132654) / 3',
            'symbol_to_path_map': {
                'va': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_unrealized_va_10s_ssp1_md5_d9b620961bfe56b7bfb52ee67babe364.tif',
                'en': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_unrealized_en_10s_ssp1_md5_2ae004b2e3559cdfc53ed754bfd6b33e.tif',
                'fo': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_unrealized_fo_10s_ssp1_md5_08c28442f699f35ab903b23480945785.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/deficit_pollination_10s_ssp1.tif",
            'build_overview': True,
        },
        {
            'expression': '(va/486980 + en/3319921 + fo/132654) / 3',
            'symbol_to_path_map': {
                'va': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_unrealized_va_10s_ssp3_md5_0a6744d0b69ec295292a84c8383290d5.tif',
                'en': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_unrealized_en_10s_ssp3_md5_10ce2f30db2ac4a97266cfd075e67fa9.tif',
                'fo': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_unrealized_fo_10s_ssp3_md5_19a2a1423c028e883a477e6b73524da5.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/deficit_pollination_10s_ssp3.tif",
            'build_overview': True,
        },
        {
            'expression': '(va/486980 + en/3319921 + fo/132654) / 3',
            'symbol_to_path_map': {
                'va': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_unrealized_va_10s_ssp5_md5_33e0cd5f3a846d1532a44c56c2d4ade5.tif',
                'en': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_unrealized_en_10s_ssp5_md5_b5fb16243689850078961e0228f774f2.tif',
                'fo': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_unrealized_fo_10s_ssp5_md5_155e5e1aab3c226a693973efc41400fc.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/deficit_pollination_10s_ssp5.tif",
            'build_overview': True,
        },
        {
            'expression': '(va/486980 + en/3319921 + fo/132654) / 3',
            'symbol_to_path_map': {
                'va': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_va_30s_cur_md5_5dc3b32361e73deefe0c1d3405d1887b.tif',
                'en': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_en_30s_cur_md5_a0216f9f217a5960179720585720d4fa.tif',
                'fo': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_fo_30s_cur_md5_01077b8ee4bae46e1d07c23728d740fc.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "pollination_pop_30s_cur.tif",
            'build_overview': True,
        },
        {
            'expression': '(va/486980 + en/3319921 + fo/132654) / 3',
            'symbol_to_path_map': {
                'va': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_en_30s_ssp1_md5_2dec3f715e60666797c3ec170ee86cce.tif',
                'en': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_en_30s_ssp1_md5_2dec3f715e60666797c3ec170ee86cce.tif',
                'fo': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_fo_30s_ssp1_md5_655aa774ebd352d5bf82336c4c4a72ab.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "pollination_pop_30s_ssp1.tif",
            'build_overview': True,
        },
        {
            'expression': '(va/486980 + en/3319921 + fo/132654) / 3',
            'symbol_to_path_map': {
                'va': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_va_30s_ssp3_md5_024b2aa9c2e71e72c246c34b71b75bf8.tif',
                'en': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_en_30s_ssp3_md5_2cd38b2e5b32238f24b635dfdd70cf22.tif',
                'fo': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_fo_30s_ssp3_md5_3f8b935a55836c44f7912f5520699179.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "pollination_pop_30s_ssp3.tif",
            'build_overview': True,
        },
        {
            'expression': '(va/486980 + en/3319921 + fo/132654) / 3',
            'symbol_to_path_map': {
                'va': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_va_30s_ssp5_md5_4267bfdd9392dff1d8cfd30f504567d9.tif',
                'en': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_en_30s_ssp5_md5_279c0ec49113c0036d3dc8c9ef387469.tif',
                'fo': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_fo_30s_ssp5_md5_8b1dfa322e4e9202711e8057a34c508e.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "pollination_pop_30s_ssp5.tif",
            'build_overview': True,
        },
        {
            'expression': '(va/486980 + en/3319921 + fo/132654) / 3',
            'symbol_to_path_map': {
                'va': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_va_30s_cur_md5_5dc3b32361e73deefe0c1d3405d1887b.tif',
                'en': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_en_30s_cur_md5_a0216f9f217a5960179720585720d4fa.tif',
                'fo': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_fo_30s_cur_md5_01077b8ee4bae46e1d07c23728d740fc.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "pollination_nut_req_30s_cur.tif",
            'build_overview': True,
        },
        {
            'expression': '(va/486980 + en/3319921 + fo/132654) / 3',
            'symbol_to_path_map': {
                'va': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_en_30s_ssp1_md5_2dec3f715e60666797c3ec170ee86cce.tif',
                'en': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_en_30s_ssp1_md5_2dec3f715e60666797c3ec170ee86cce.tif',
                'fo': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_fo_30s_ssp1_md5_655aa774ebd352d5bf82336c4c4a72ab.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "pollination_nut_req_30s_ssp1.tif",
            'build_overview': True,
        },
        {
            'expression': '(va/486980 + en/3319921 + fo/132654) / 3',
            'symbol_to_path_map': {
                'va': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_va_30s_ssp3_md5_024b2aa9c2e71e72c246c34b71b75bf8.tif',
                'en': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_en_30s_ssp3_md5_2cd38b2e5b32238f24b635dfdd70cf22.tif',
                'fo': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_fo_30s_ssp3_md5_3f8b935a55836c44f7912f5520699179.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "pollination_nut_req_30s_ssp3.tif",
            'build_overview': True,
        },
        {
            'expression': '(va/486980 + en/3319921 + fo/132654) / 3',
            'symbol_to_path_map': {
                'va': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_va_30s_ssp5_md5_4267bfdd9392dff1d8cfd30f504567d9.tif',
                'en': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_en_30s_ssp5_md5_279c0ec49113c0036d3dc8c9ef387469.tif',
                'fo': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/nut_req_fo_30s_ssp5_md5_8b1dfa322e4e9202711e8057a34c508e.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "pollination_nut_req_30s_ssp5.tif",
            'build_overview': True,
        },
    ]

    for calculation in raster_calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()

    derived_raster_calculation_list = [
        {
            'expression': '(potential-deficit)/potential',
            'symbol_to_path_map': {
                'potential': 'outputs/potential_pollination_10s_cur.tif',
                'deficit': 'outputs/deficit_pollination_10s_cur.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/NC_pollination_10s_cur.tif",
            'build_overview': True,
        },
        {
            'expression': '(potential-deficit)/potential',
            'symbol_to_path_map': {
                'potential': 'outputs/potential_pollination_10s_ssp1.tif',
                'deficit': 'outputs/deficit_pollination_10s_ssp1.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/NC_pollination_10s_ssp1.tif",
            'build_overview': True,
        },
        {
            'expression': '(potential-deficit)/potential',
            'symbol_to_path_map': {
                'potential': 'outputs/potential_pollination_10s_ssp3.tif',
                'deficit': 'outputs/deficit_pollination_10s_ssp3.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/NC_pollination_10s_ssp3.tif",
            'build_overview': True,
        },
        {
            'expression': '(potential-deficit)/potential',
            'symbol_to_path_map': {
                'potential': 'outputs/potential_pollination_10s_ssp5.tif',
                'deficit': 'outputs/deficit_pollination_10s_ssp5.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/NC_pollination_10s_ssp5.tif",
            'build_overview': True,
        },

    ]



    raster_change_calculation_list = [
        {
            'expression': '(future-current)/current',
            'symbol_to_path_map': {
                'future': 'outputs/NC_nutrient_10s_ssp1.tif',
                'current': 'outputs/NC_nutrient_10s_cur.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/NCchange_nutrient_10s_ssp1.tif",
            'build_overview': True,
        },
        {
            'expression': '(future-current)/current',
            'symbol_to_path_map': {
                'future': 'outputs/NC_nutrient_10s_ssp3.tif',
                'current': 'outputs/NC_nutrient_10s_cur.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/NCchange_nutrient_10s_ssp3.tif",
            'build_overview': True,
        },
        {
            'expression': '(future-current)/current',
            'symbol_to_path_map': {
                'future': 'outputs/NC_nutrient_10s_ssp5.tif',
                'current': 'outputs/NC_nutrient_10s_cur.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/NCchange_nutrient_10s_ssp5.tif",
            'build_overview': True,
        },
        {
            'expression': '(future-current)/current',
            'symbol_to_path_map': {
                'future': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2050_ssp1_modified_load_compressed_md5_a5f1db75882a207636546af94cde6549.tif',
                'current': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2015_modified_load_compressed_md5_e3072705a87b0db90e7620abbc0d75f1.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/potential_change_nutrient_10s_ssp1.tif",
            'build_overview': True,
        },
        {
            'expression': '(future-current)/current',
            'symbol_to_path_map': {
                'future': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2050_ssp3_modified_load_compressed_md5_e49e578ed025c0bc796e55b7f27f82f1.tif',
                'current': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2015_modified_load_compressed_md5_e3072705a87b0db90e7620abbc0d75f1.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/potential_change_nutrient_10s_ssp3.tif",
            'build_overview': True,
        },
        {
            'expression': '(future-current)/current',
            'symbol_to_path_map': {
                'future': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2050_ssp5_modified_load_compressed_md5_7337576433238f70140be9ec5b588fd1.tif',
                'current': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2015_modified_load_compressed_md5_e3072705a87b0db90e7620abbc0d75f1.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/potential_change_nutrient_10s_ssp5.tif",
            'build_overview': True,
        },
        {
            'expression': '(future-current)/current',
            'symbol_to_path_map': {
                'future': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2050_ssp1_n_export_compressed_md5_4b2b0a4ac6575fde5aca00de4f788494.tif',
                'current': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2015_n_export_compressed_md5_fa15687cc4d4fdc5e7a6351200873578.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/deficit_change_nutrient_10s_ssp1.tif",
            'build_overview': True,
        },
        {
            'expression': '(future-current)/current',
            'symbol_to_path_map': {
                'future': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2050_ssp3_n_export_compressed_md5_b5259ac0326b0dcef8a34f2086e8339b.tif',
                'current': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2015_n_export_compressed_md5_fa15687cc4d4fdc5e7a6351200873578.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/deficit_change_nutrient_10s_ssp3.tif",
            'build_overview': True,
        },
        {
            'expression': '(future-current)/current',
            'symbol_to_path_map': {
                'future': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2050_ssp5_n_export_compressed_md5_12b9caecc29058d39748e13bf5b5f150.tif',
                'current': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_2015_n_export_compressed_md5_fa15687cc4d4fdc5e7a6351200873578.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/deficit_change_nutrient_10s_ssp5.tif",
            'build_overview': True,
        },
        {
            'expression': '(future-current)/current',
            'symbol_to_path_map': {
                'future': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/water_ruralpop_30s_ssp1_md5_203296fc1aeb46fea41f2e46cf49a66f.tif',
                'current': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/water_ruralpop_30s_2015_md5_2fbcc65a4b24bc4fe3c2d6cfc263fe63.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/pop_change_nutrient_30s_ssp1.tif",
            'build_overview': True,
        },
        {
            'expression': '(future-current)/current',
            'symbol_to_path_map': {
                'future': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/water_ruralpop_30s_ssp3_md5_9d4abd78e5d96d7e28282eee926af3f0.tif',
                'current': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/water_ruralpop_30s_2015_md5_2fbcc65a4b24bc4fe3c2d6cfc263fe63.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/pop_change_nutrient_30s_ssp3.tif",
            'build_overview': True,
        },
        {
            'expression': '(future-current)/current',
            'symbol_to_path_map': {
                'future': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/water_ruralpop_30s_ssp5_md5_72e336f1fa276118999807e4011cae50.tif',
                'current': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/water_ruralpop_30s_2015_md5_2fbcc65a4b24bc4fe3c2d6cfc263fe63.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/pop_change_nutrient_30s_ssp5.tif",
            'build_overview': True,
        },
        {
            'expression': '(future-current)/current',
            'symbol_to_path_map': {
                'future': 'outputs/NC_pollination_10s_ssp1.tif',
                'current': 'outputs/NC_pollination_10s_cur.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/NCchange_pollination_10s_ssp1.tif",
            'build_overview': True,
        },
        {
            'expression': '(future-current)/current',
            'symbol_to_path_map': {
                'future': 'outputs/NC_pollination_10s_ssp3.tif',
                'current': 'outputs/NC_pollination_10s_cur.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/NCchange_pollination_10s_ssp3.tif",
            'build_overview': True,
        },
        {
            'expression': '(future-current)/current',
            'symbol_to_path_map': {
                'future': 'outputs/NC_pollination_10s_ssp5.tif',
                'current': 'outputs/NC_pollination_10s_cur.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/NCchange_pollination_10s_ssp5.tif",
            'build_overview': True,
        },
        {
            'expression': '(future-current)/current',
            'symbol_to_path_map': {
                'future': 'outputs/potential_pollination_10s_ssp1.tif',
                'current': 'outputs/potential_pollination_10s_cur.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/potential_change_pollination_10s_ssp1.tif",
            'build_overview': True,
        },
        {
            'expression': '(future-current)/current',
            'symbol_to_path_map': {
                'future': 'outputs/potential_pollination_10s_ssp3.tif',
                'current': 'outputs/potential_pollination_10s_cur.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/potential_change_pollination_10s_ssp3.tif",
            'build_overview': True,
        },
        {
            'expression': '(future-current)/current',
            'symbol_to_path_map': {
                'future': 'outputs/potential_pollination_10s_ssp5.tif',
                'current': 'outputs/potential_pollination_10s_cur.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/potential_change_pollination_10s_ssp5.tif",
            'build_overview': True,
        },
        {
            'expression': '(future-current)/current',
            'symbol_to_path_map': {
                'future': 'outputs/deficit_pollination_10s_ssp1.tif',
                'current': 'outputs/deficit_pollination_10s_cur.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/deficit_change_pollination_10s_ssp1.tif",
            'build_overview': True,
        },
        {
            'expression': '(future-current)/current',
            'symbol_to_path_map': {
                'future': 'outputs/deficit_pollination_10s_ssp3.tif',
                'current': 'outputs/deficit_pollination_10s_cur.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/deficit_change_pollination_10s_ssp3.tif",
            'build_overview': True,
        },
        {
            'expression': '(future-current)/current',
            'symbol_to_path_map': {
                'future': 'outputs/deficit_pollination_10s_ssp5.tif',
                'current': 'outputs/deficit_pollination_10s_cur.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/deficit_change_pollination_10s_ssp5.tif",
            'build_overview': True,
        },
    ]

    for calculation in derived_raster_calculation_list+raster_change_calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()





if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(WORKSPACE_DIR, NCPUS, 5.0)
    main()
