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

    raster_calculation_list = [
        {
            'expression': '(va/486980 + en/3319921 + fo/132654) / 3',
            'symbol_to_path_map': {
                'va': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_potential_va_10s_cur_md5_8e327c260369864d5a38e03279574fb2.tif',
                'en': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_potential_en_10s_cur_md5_a33bd27cb092807455812b6474b88ea3.tif',
                'fo': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/prod_poll_dep_potential_fo_10s_cur_md5_f0660f3e3123ed1b64a502046e4246bd.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/pollination_potential_10s_cur.tif",
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
            'target_raster_path': "outputs/pollination_potential_10s_ssp1.tif",
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
            'target_raster_path': "outputs/pollination_potential_10s_ssp3.tif",
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
            'target_raster_path': "outputs/pollination_potential_10s_ssp5.tif",
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
            'target_raster_path': "outputs/pollination_deficit_10s_cur.tif",
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
            'target_raster_path': "outputs/pollination_deficit_10s_ssp1.tif",
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
            'target_raster_path': "outputs/pollination_deficit_10s_ssp3.tif",
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
            'target_raster_path': "outputs/pollination_deficit_10s_ssp5.tif",
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
            'target_raster_path': "outputs/pollination_nut_req_30s_cur.tif",
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
            'target_raster_path': "outputs/pollination_nut_req_30s_ssp1.tif",
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
            'target_raster_path': "outputs/pollination_nut_req_30s_ssp3.tif",
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
            'target_raster_path': "outputs/pollination_nut_req_30s_ssp5.tif",
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
                'potential': 'outputs/pollination_potential_10s_cur.tif',
                'deficit': 'outputs/pollination_deficit_10s_cur.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/pollination_NC_10s_cur.tif",
            'build_overview': True,
        },
        {
            'expression': '(potential-deficit)/potential',
            'symbol_to_path_map': {
                'potential': 'outputs/pollination_potential_10s_ssp1.tif',
                'deficit': 'outputs/pollination_deficit_10s_ssp1.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/pollination_NC_10s_ssp1.tif",
            'build_overview': True,
        },
        {
            'expression': '(potential-deficit)/potential',
            'symbol_to_path_map': {
                'potential': 'outputs/pollination_potential_10s_ssp3.tif',
                'deficit': 'outputs/pollination_deficit_10s_ssp3.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/pollination_NC_10s_ssp3.tif",
            'build_overview': True,
        },
        {
            'expression': '(potential-deficit)/potential',
            'symbol_to_path_map': {
                'potential': 'outputs/pollination_potential_10s_ssp5.tif',
                'deficit': 'outputs/pollination_deficit_10s_ssp5.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "outputs/pollination_NC_10s_ssp5.tif",
            'build_overview': True,
        },
    ]



    for calculation in derived_raster_calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()





if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(WORKSPACE_DIR, NCPUS, 5.0)
    main()
