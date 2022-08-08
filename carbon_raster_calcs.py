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

python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",12 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",11 --sub
python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",11 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",10 --sub
python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",10 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",9 --sub
python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",9 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",8 --sub
python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",8 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",7 --sub
python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",7 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",6 --sub
python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",6 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",5 --sub
python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",5 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",4 --sub
python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",4 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",3 --sub
python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",3 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",2 --sub
python add_sub_missing_as_0.py "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",2 "D:\Documents\unilever_archive\baccini_carbon_data_2003_2014_md5_11d145_b.tif",1 --sub

    calculation_list = [
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2000_compressed_full_forest_edge_result.tif",
                'raster2': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2000_compressed_no_forest_edge_result.tif",
            },
            'target_nodata': 0,
            #'target_pixel_size': (0.002777777777777777884,-0.002777777777777777884),
            #'resample_method': 'near',
            'target_raster_path': r"D:\Documents\unilever_archive\carbon_rasters_2022\DIFF_forest_edge_carbon_2000.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2001_compressed_full_forest_edge_result.tif",
                'raster2': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2001_compressed_no_forest_edge_result.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\Documents\unilever_archive\carbon_rasters_2022\DIFF_forest_edge_carbon_2001.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2002_compressed_full_forest_edge_result.tif",
                'raster2': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2002_compressed_no_forest_edge_result.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\Documents\unilever_archive\carbon_rasters_2022\DIFF_forest_edge_carbon_2002.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2003_compressed_full_forest_edge_result.tif",
                'raster2': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2003_compressed_no_forest_edge_result.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\Documents\unilever_archive\carbon_rasters_2022\DIFF_forest_edge_carbon_2003.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2004_compressed_full_forest_edge_result.tif",
                'raster2': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2004_compressed_no_forest_edge_result.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\Documents\unilever_archive\carbon_rasters_2022\DIFF_forest_edge_carbon_2004.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2004_compressed_std_forest_edge_result.tif",
                'raster2': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2003_compressed_std_forest_edge_result.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\Documents\unilever_archive\carbon_rasters_2022\DIFF_forest_carbon_2004-2003.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2003_compressed_std_forest_edge_result.tif",
                'raster2': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2002_compressed_std_forest_edge_result.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\Documents\unilever_archive\carbon_rasters_2022\DIFF_forest_carbon_2003-2002.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2002_compressed_std_forest_edge_result.tif",
                'raster2': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2001_compressed_std_forest_edge_result.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\Documents\unilever_archive\carbon_rasters_2022\DIFF_forest_carbon_2002-2001.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2001_compressed_std_forest_edge_result.tif",
                'raster2': r"D:\Documents\unilever_archive\carbon_rasters_2022\fc_stack_hansen_forest_cover2000_compressed_std_forest_edge_result.tif",
            },
            'target_nodata': 0,
            'target_raster_path': r"D:\Documents\unilever_archive\carbon_rasters_2022\DIFF_forest_carbon_2001-2000.tif",
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
