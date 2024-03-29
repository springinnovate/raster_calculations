"""These calculations are for the carbon regression project."""

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

     #Don't forget the r before the "  !



    single_expression = {
        'expression': '(raster1>0)*raster1',
        'symbol_to_path_map': {
            'raster1': r"D:\repositories\carbon_edge_model\fc_stack_hansen_2014_missing_carbon_compressed_nr_md5_63366a.tif",
        },
        'target_nodata': -1,
        'target_raster_path': "fc_stack_hansen_2014_missing_carbon_positive_only.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': '(raster1<-10000)*65535 + (raster1>-10000)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\carbon_edge_model\DIFF_forest_carbon_ipcc-baccini.tif",
            },
            'target_nodata': 65535,
            'target_raster_path': "DIFF_forest_carbon_ipcc-baccini_c.tif",
        },
        {
            'expression': '(raster1<-10000)*65535 + (raster1>-10000)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\carbon_edge_model\DIFF_forest_carbon_regression-baccini.tif",
            },
            'target_nodata': 65535,
            'target_raster_path': "DIFF_forest_carbon_regression-baccini_c.tif",
        }
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()

    calculation_list = [
        {
            'expression': '(raster1>-10000)*raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\carbon_edge_model\DIFF_forest_carbon_regression-baccini_c_nr_md5_890f1c.tif",
                'raster2': r"D:\repositories\carbon_edge_model\DIFF_forest_carbon_ipcc-baccini_c_nr_md5_cfd45b.tif",
            },
            'target_nodata': 66535,
            'target_raster_path': "DIFF_forest_carbon_ipcc-baccini_c_regression_extent.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': 'raster1*(raster2<4)',
        'symbol_to_path_map': {
            'raster1': r"D:\repositories\carbon_edge_model\output_global\regression_optimization\regressioncoarsened_marginal_value_regression_mask_3500000280000.0.tif",
            'raster2': r"D:\repositories\carbon_edge_model\supporting_data\LPD_WARPED_near_md5_539b465b3a66b18060af7b7f702544e7.tif",
        },
        'target_nodata': 127,
        'default_nan': 127,
        'target_raster_path': "degraded_lands_on_regression_350mha.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return



    calculation_list = [
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\carbon_edge_model\output_global\regression_carbon_esa.tif",
                'raster2': r"D:\repositories\raster_calculations\align_to_mask_workspace\baccini_carbon_data_2014_compressed_WARPED_near_md5_38d78c7aa3e61d7314f3e78edc28ed2a.tif",
            },
            'target_nodata': 65535,
            'target_raster_path': r"D:\repositories\carbon_edge_model\DIFF_forest_carbon_regression-baccini.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\carbon_edge_model\output_global\ipcc_carbon_esa.tif",
                'raster2': r"D:\repositories\raster_calculations\align_to_mask_workspace\baccini_carbon_data_2014_compressed_WARPED_near_md5_38d78c7aa3e61d7314f3e78edc28ed2a.tif",
            },
            'target_nodata': 65535,
            'target_raster_path': r"D:\repositories\carbon_edge_model\DIFF_forest_carbon_ipcc-baccini.tif",
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
            'expression': 'raster1-raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\carbon_edge_model\c_stack_hansen_forest_cover2014_compressed_full_forest_edge_result.tif",
                'raster2': r"D:\repositories\carbon_edge_model\c_stack_hansen_forest_cover2014_compressed_std_forest_edge_result.tif",
            },
            'target_nodata': 0,
            'target_raster_path': "hansen_2014_missing_carbon.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)


    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return



    #python -m ecoshard process "fc_stack_hansen_forest_cover2003_compressed_std_forest_edge_result.tif" --reduce_factor 8 average ./reduce_factor/fc_stack_hansen_forest_cover2003_carbon_reduce8x.tif
    #python -m ecoshard process "fc_stack_hansen_forest_cover2004_compressed_std_forest_edge_result.tif" --reduce_factor 8 average ./reduce_factor/fc_stack_hansen_forest_cover2004_carbon_reduce8x.tif
    #python -m ecoshard process "fc_stack_hansen_forest_cover2006_compressed_std_forest_edge_result.tif" --reduce_factor 8 average ./reduce_factor/fc_stack_hansen_forest_cover2006_carbon_reduce8x.tif
    #python -m ecoshard process "fc_stack_hansen_forest_cover2007_compressed_std_forest_edge_result.tif" --reduce_factor 8 average ./reduce_factor/fc_stack_hansen_forest_cover2007_carbon_reduce8x.tif
    #python -m ecoshard process "fc_stack_hansen_forest_cover2008_compressed_std_forest_edge_result.tif" --reduce_factor 8 average ./reduce_factor/fc_stack_hansen_forest_cover2008_carbon_reduce8x.tif
    #python -m ecoshard process "fc_stack_hansen_forest_cover2009_compressed_std_forest_edge_result.tif" --reduce_factor 8 average ./reduce_factor/fc_stack_hansen_forest_cover2009_carbon_reduce8x.tif
    #python -m ecoshard process "fc_stack_hansen_forest_cover2010_compressed_std_forest_edge_result.tif" --reduce_factor 8 average ./reduce_factor/fc_stack_hansen_forest_cover2010_carbon_reduce8x.tif
    #python -m ecoshard process "fc_stack_hansen_forest_cover2011_compressed_std_forest_edge_result.tif" --reduce_factor 8 average ./reduce_factor/fc_stack_hansen_forest_cover2011_carbon_reduce8x.tif
    #python -m ecoshard process "fc_stack_hansen_forest_cover2012_compressed_std_forest_edge_result.tif" --reduce_factor 8 average ./reduce_factor/fc_stack_hansen_forest_cover2012_carbon_reduce8x.tif
    #python -m ecoshard process "fc_stack_hansen_forest_cover2013_compressed_std_forest_edge_result.tif" --reduce_factor 8 average ./reduce_factor/fc_stack_hansen_forest_cover2013_carbon_reduce8x.tif
    #python -m ecoshard process "fc_stack_hansen_forest_cover2014_compressed_std_forest_edge_result.tif" --reduce_factor 8 average ./reduce_factor/fc_stack_hansen_forest_cover2014_carbon_reduce8x.tif

# Don't actually want to do it this way because the biggest errors will be where baccini has data and fc_stack doesn't, which is not what we're interested in at all
#python add_sub_missing_as_0.py "D:\Documents\unilever_archive\carbon_rasters_2022\reduce_factor\fc_stack_hansen_forest_cover2003_carbon_reduce8x.tif" "D:\Documents\unilever_archive\carbon_edge_model\carbon_model_workspace\data\baccini_carbon_data_2003_2014_compressed_md5_11d1455ee8f091bf4be12c4f7ff9451b.tif",1 --sub

    calculation_list = [
        #{
        #    'expression': 'raster1 - raster2',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2000_compressed_full_forest_edge_result.tif",
        #        'raster2': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2000_compressed_no_forest_edge_result.tif",
        #    },
        #    'target_nodata': 0,
        #    'target_raster_path': r"D:\Documents\unilever_archive\carbon_rasters_2022\DIFF_forest_edge_carbon_2000.tif",
        #},
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\carbon_edge_model\fc_stack_hansen_forest_cover2007_compressed_std_forest_edge_result.tif",
                'raster2': r"D:\repositories\carbon_edge_model\fc_stack_hansen_forest_cover2006_compressed_std_forest_edge_result.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\carbon_edge_model\DIFF_forest_carbon_2007-2006.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\carbon_edge_model\fc_stack_hansen_forest_cover2013_compressed_std_forest_edge_result.tif",
                'raster2': r"D:\repositories\carbon_edge_model\fc_stack_hansen_forest_cover2012_compressed_std_forest_edge_result.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\carbon_edge_model\DIFF_forest_carbon_2013-2012.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\carbon_edge_model\fc_stack_hansen_forest_cover2012_compressed_std_forest_edge_result.tif",
                'raster2': r"D:\repositories\carbon_edge_model\fc_stack_hansen_forest_cover2011_compressed_std_forest_edge_result.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\carbon_edge_model\DIFF_forest_carbon_2012-2011.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\carbon_edge_model\fc_stack_hansen_forest_cover2011_compressed_std_forest_edge_result.tif",
                'raster2': r"D:\repositories\carbon_edge_model\fc_stack_hansen_forest_cover2010_compressed_std_forest_edge_result.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\carbon_edge_model\DIFF_forest_carbon_2011-2010.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\carbon_edge_model\fc_stack_hansen_forest_cover2010_compressed_std_forest_edge_result.tif",
                'raster2': r"D:\repositories\carbon_edge_model\fc_stack_hansen_forest_cover2009_compressed_std_forest_edge_result.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\carbon_edge_model\DIFF_forest_carbon_2010-2009.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\carbon_edge_model\fc_stack_hansen_forest_cover2009_compressed_std_forest_edge_result.tif",
                'raster2': r"D:\repositories\carbon_edge_model\fc_stack_hansen_forest_cover2008_compressed_std_forest_edge_result.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\carbon_edge_model\DIFF_forest_carbon_2009-2008.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\carbon_edge_model\fc_stack_hansen_forest_cover2008_compressed_std_forest_edge_result.tif",
                'raster2': r"D:\repositories\carbon_edge_model\fc_stack_hansen_forest_cover2007_compressed_std_forest_edge_result.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\carbon_edge_model\DIFF_forest_carbon_2008-2007.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\carbon_edge_model\fc_stack_hansen_forest_cover2007_compressed_std_forest_edge_result.tif",
                'raster2': r"D:\repositories\carbon_edge_model\fc_stack_hansen_forest_cover2006_compressed_std_forest_edge_result.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\repositories\carbon_edge_model\DIFF_forest_carbon_2007-2006.tif",
        },
        #{
        #    'expression': 'raster1 - raster2',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2004_compressed_std_forest_edge_result.tif",
        #        'raster2': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2003_compressed_std_forest_edge_result.tif",
        #    },
        #    'target_nodata': 0,
        #    'target_raster_path': r"D:\Documents\unilever_archive\carbon_rasters_2022\DIFF_forest_carbon_2004-2003.tif",
        #},
        #{
        #    'expression': 'raster1 - raster2',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2003_compressed_std_forest_edge_result.tif",
        #        'raster2': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2002_compressed_std_forest_edge_result.tif",
        #    },
        #    'target_nodata': 0,
        #    'target_raster_path': r"D:\Documents\unilever_archive\carbon_rasters_2022\DIFF_forest_carbon_2003-2002.tif",
        #},
        #{
        #    'expression': 'raster1 - raster2',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2002_compressed_std_forest_edge_result.tif",
        #        'raster2': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2001_compressed_std_forest_edge_result.tif",
        #    },
        #    'target_nodata': 0,
        #    'target_raster_path': r"D:\Documents\unilever_archive\carbon_rasters_2022\DIFF_forest_carbon_2002-2001.tif",
        #},
        #{
        #    'expression': 'raster1 - raster2',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2001_compressed_std_forest_edge_result.tif",
        #        'raster2': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2000_compressed_std_forest_edge_result.tif",
        #    },
        #    'target_nodata': 0,
        #    'target_raster_path': r"D:\Documents\unilever_archive\carbon_rasters_2022\DIFF_forest_carbon_2001-2000.tif",
        #},
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    #python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",12 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",11 --sub
    #python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",11 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",10 --sub
    #python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",10 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",9 --sub
    #python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",9 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",8 --sub
    #python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",8 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",7 --sub
    #python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",7 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",6 --sub
    #python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",6 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",5 --sub
    #python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",5 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",4 --sub
    #python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",4 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",3 --sub
    #python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",3 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",2 --sub
    #python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",2 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",1 --sub

if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(WORKSPACE_DIR, NCPUS, 5.0)
    main()
