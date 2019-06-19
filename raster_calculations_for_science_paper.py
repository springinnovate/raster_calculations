"""These calculations are for the science paper."""
import sys
import os
import logging
import multiprocessing

import raster_calculations
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

    # Becky, here's an example of how to use mask:
    mask_test = {
        'expression': 'mask(raster, 1, 2, 3, 5, invert=False)',
        'symbol_to_path_map': {
            'raster': r"C:\Users\rpsharp\Documents\bitbucket_repos\invest\data\invest-sample-data\Base_Data\Freshwater\landuse_90",
        },
        'target_nodata': -1,
        'target_raster_path': 'masked.tif',
    }
    raster_calculations.evaluate_calculation(
        mask_test, TASK_GRAPH, WORKSPACE_DIR)
    TASK_GRAPH.join()
    TASK_GRAPH.close()
    return

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
    ]

    for calculation in raster_calculation_list:
        raster_calculations.evaluate_calculation(
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
        raster_calculations.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()


if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(WORKSPACE_DIR, NCPUS, 5.0)
    main()
