"""These calculations are GEOBON analysis (and side-project for Tasya at the bottom)."""
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
import pygeoprocessing
import numpy

gdal.SetCacheMax(2**30)

WORKSPACE_DIR = 'rastercalc_workspace'
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


#############GEOBON###########################

    single_expression = {
        'expression': '(raster1>0)*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\cnc_project\original_rasters\cnc_cv\normalized_pop_on_hab\total_pop_masked_by_10m_md5_ef02b7ee48fa100f877e3a1671564be2.tif",
            'raster2': r"C:\Users\Becky\Documents\raster_calculations\lspop2000_md5_79a872e3480c998a4a8bfa28feee228c.tif",
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_pixel_size': (0.0083333333333333,-0.0083333333333333),
        'resample_method': 'near',
        'target_raster_path': "total_pop_masked_by_10m_2000.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': 'raster1*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\geobon\CV\pnv_lspop2017\cv_value_pnv_md5_3e1680fd99db84773e1473289958e0ac.tif",
            'raster2': r"C:\Users\Becky\Documents\geobon\CV\pnv_lspop2017\cv_pop_pnv_md5_57ca9a7a91fe23a81c549d17adf6dbd1.tif",
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_pixel_size': (0.0027777778,-0.0027777778),
        'resample_method': 'near',
        'target_raster_path': "coastal_risk_reduction_pnvls17.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calc_list = [
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\geobon\ndr\stitch_pnv_esa_modified_load.tif",
                'raster2': r"C:\Users\Becky\Documents\geobon\ndr\stitch_pnv_esa_n_export.tif",
            },
            'target_nodata': float(numpy.finfo(numpy.float32).min),
            'default_nan': float(numpy.finfo(numpy.float32).min),
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "pnv_n_retention.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\geobon\ndr\stitch_worldclim_esa_2000_modified_load.tif",
                'raster2': r"C:\Users\Becky\Documents\geobon\ndr\stitch_worldclim_esa_2000_n_export.tif",
            },
            'target_nodata': float(numpy.finfo(numpy.float32).min),
            'default_nan': float(numpy.finfo(numpy.float32).min),
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "esa2000_n_retention.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\geobon\ndr\stitch_worldclim_esa_2015_modified_load.tif",
                'raster2': r"C:\Users\Becky\Documents\geobon\ndr\stitch_worldclim_esa_2015_n_export.tif",
            },
            'target_nodata': float(numpy.finfo(numpy.float32).min),
            'default_nan': float(numpy.finfo(numpy.float32).min),
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "esa2015_n_retention.tif",
        },

    ]
    for calc in calc_list:
       raster_calculations_core.evaluate_calculation(
            calc, TASK_GRAPH, WORKSPACE_DIR)


    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    single_expression = {
        'expression': 'raster1*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\geobon\CV\2000_with_lspop2017\cv_value_esa2000ls17.tif",
            'raster2': r"C:\Users\Becky\Documents\geobon\CV\2000_with_lspop2017\cv_pop_esa2000ls17.tif",
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_pixel_size': (0.00277777780000000021,-0.00277777780000000021),
        'resample_method': 'near',
        'target_raster_path': "coastal_risk_reduction_esa2000ls17.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)

    single_expression = {
        'expression': 'raster1*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\geobon\CV\2000\cv_value_esa2000.tif",
            'raster2': r"C:\Users\Becky\Documents\geobon\CV\2000\cv_pop_esa2000.tif",
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_pixel_size': (0.00277777780000000021,-0.00277777780000000021),
        'resample_method': 'near',
        'target_projection_wkt': wgs84_srs.ExportToWkt(),
        'target_raster_path': "coastal_risk_reduction_esa2000.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': 'raster1*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\geobon\CV\2000_with_lspop2017\cv_value_esa2000ls17.tif",
            'raster2': r"C:\Users\Becky\Documents\geobon\CV\2000_with_lspop2017\cv_pop_esa2000ls17.tif",
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_pixel_size': (0.00277777780000000021,-0.00277777780000000021),
        'resample_method': 'near',
        'target_raster_path': "coastal_risk_reduction_esa2000ls17.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': 'raster1*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\geobon\CV\2018\cv_value_esa2018.tif",
            'raster2': r"C:\Users\Becky\Documents\geobon\CV\2018\cv_pop_esa2018.tif",
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_raster_path': "coastal_risk_reduction_esa2018.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    single_expression = {
        'expression': 'raster1*raster2*raster3*(raster4>0)+(raster4<1)*-9999',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\geobon\pollination\monfreda_2008_yield_poll_dep_ppl_fed_5min.tif",
            'raster2': r"C:\Users\Becky\Documents\geobon\pollination\poll_suff_ag_coverage_prop_10s_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2018-v2.1.1.tif",
            'raster3': r"C:\Users\Becky\Documents\geobon\pollination\esa_pixel_area_ha.tif",
            'raster4': r"C:\Users\Becky\Documents\geobon\pollination\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2018-v2.1.1_ag_mask.tif"
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_pixel_size': (0.00277777780000000021,-0.00277777780000000021),
        'resample_method': 'near',
        'target_raster_path': "pollination_ppl_fed_on_ag_10s_esa2018.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': 'raster1*raster2*raster3*(raster4>0)+(raster4<1)*-9999',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\geobon\pollination\monfreda_2008_yield_poll_dep_ppl_fed_5min.tif",
            'raster2': r"C:\Users\Becky\Documents\geobon\pollination\poll_suff_ag_coverage_prop_10s_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2000-v2.0.7.tif",
            'raster3': r"C:\Users\Becky\Documents\geobon\pollination\esa_pixel_area_ha.tif",
            'raster4': r"C:\Users\Becky\Documents\geobon\pollination\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2000-v2.0.7_ag_mask.tif"
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_pixel_size': (0.00277777780000000021,-0.00277777780000000021),
        'resample_method': 'near',
        'target_raster_path': "pollination_ppl_fed_on_ag_10s_esa2000.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    single_expression = {
        'expression': 'raster1*raster2',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\geobon\pollination\monfreda_2008_yield_poll_dep_ppl_fed_5min.tif",
            'raster2': r"C:\Users\Becky\Documents\geobon\pollination\esa_pixel_area_ha.tif",

        },
        'target_nodata': float(numpy.finfo(numpy.float32).min),
        'default_nan': float(numpy.finfo(numpy.float32).min),
        'target_pixel_size': (0.00277777780000000021,-0.00277777780000000021),
        'resample_method': 'near',
        'target_raster_path': "monfreda_prod_poll_dep_ppl_fed_10sec.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': '10000(va/486980 + en/3319921 + fo/132654) / 3', # not sure why but this is 10,000 x smaller than previous version
        'symbol_to_path_map': {
            'en': r"C:\Users\Becky\Documents\raster_calculations\ag_work\pollination\monfreda_2008_yield_poll_dep_en_10km_md5_a9511553677951a7d65ebe0c4628c94b.tif",
            'fo': r"C:\Users\Becky\Documents\raster_calculations\ag_work\pollination\monfreda_2008_yield_poll_dep_fo_10km_md5_20f06155618f3ce088e7796810a0c747.tif",
            'va': r"C:\Users\Becky\Documents\raster_calculations\ag_work\pollination\monfreda_2008_yield_poll_dep_va_10km_md5_3e38e4a811f79c75499e759ccebec6fc.tif",
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_raster_path': "monfreda_2008_yield_poll_dep_ppl_fed_5min.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    #For Tanya
    single_expression = {
        'expression': 'raster1*raster2*raster3*(raster4>0)+(raster4<1)*-9999',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\geobon\pollination\monfreda_2008_yield_poll_dep_ppl_fed_5min.tif",
            'raster2': r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\SEA\poll_suff_ag_coverage_prop_10s_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015_SEAclip_wgs.tif",
            'raster3': r"C:\Users\Becky\Documents\geobon\pollination\esa_pixel_area_ha.tif",
            'raster4': r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\SEA\ESA2015_without5_8forest_ag_mask.tif"
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_pixel_size': (0.002777777777778,0.002777777777778),
        'resample_method': 'near',
        'target_raster_path': "pollination_ppl_fed_on_ag_10s_esa2015_SEAclip.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': 'raster1*raster2*raster3*(raster4>0)+(raster4<1)*-9999',
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\geobon\pollination\monfreda_2008_yield_poll_dep_ppl_fed_5min.tif",
            'raster2': r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\SEA\poll_suff_ag_coverage_prop_10s_ESA2015_without5_8forest.tif",
            'raster3': r"C:\Users\Becky\Documents\geobon\pollination\esa_pixel_area_ha.tif",
            'raster4': r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\SEA\ESA2015_without5_8forest_ag_mask.tif"
        },
        'target_nodata': -9999,
        'default_nan': -9999,
        'target_pixel_size': (0.002777777777778,0.002777777777778),
        'resample_method': 'near',
        'target_raster_path': "pollination_ppl_fed_on_ag_10s_esa2015_without5_8forest.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)

    raster_calculation_list = [
        {
            'expression': '(raster2)*200 + (raster2<1)*raster1', #this resets everywhere it's a forest project to "bare"
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cnc_project\SEA\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015_SEAclip.tif",
                'raster2':r"C:\Users\Becky\Documents\cnc_project\SEA\ForestMask_5_8.tif"
            },
            'target_nodata': 0,
            'target_projection_wkt': wgs84_srs.ExportToWkt(),
            'target_pixel_size': (0.002777777777778,0.002777777777778),
            'resample_method': 'near',
            'target_raster_path': "Forest_5_8_toBare.tif",
        },
    ]

    for calculation in raster_calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    #then will need to use nodata_replace.py-> can't just do this on the mask to begin with because it's not in the right projection
    # python nodata_replace.py "C:\Users\Becky\Documents\cnc_project\SEA\Forest_5_8_toBare.tif" "C:\Users\Becky\Documents\cnc_project\SEA\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015_SEAclip.tif" ESA2015_without5_8forest.tif
    #nodata_replace doesn't work because the two rasters are slightly different dimensions. so try this:
    #docker run -it -v "%CD%":/usr/local/workspace therealspring/inspring:latest ./stitch_rasters.py --target_projection_epsg 4326 --target_cell_size 0.002777777777778 --target_raster_path ESA2015_without5_8forest.tif --resample_method near --area_weight_m2_to_wgs84 --overlap_algorithm replace --raster_pattern ./CNC_workspace/SEA/ "*wgs.tif"
    #then run pollination model
    #docker run -d --name pollsuff_container --rm -v `pwd`:/usr/local/workspace therealspring/inspring:latest make_poll_suff.py ./*.tif && docker logs pollsuff_container -f

    return


if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(WORKSPACE_DIR, NCPUS, 5.0)
    main()

