"""These calculations are for the science paper."""
import sys
import os
import logging
import multiprocessing

import raster_calculations_core
from osgeo import gdal
import taskgraph

gdal.SetCacheMax(2**30)

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



def main():
    """Write your expression here."""
    
    masker_list = [
         {
            # the %s is a placeholder for the string we're passing it using this function that lists every number in the range and takes away the [] of the list and turns it into a string
            'expression': 'mask(raster, %s, invert=False)'%(str([]+[x for x in range(50,181)])[1:-1]),
            'symbol_to_path_map': {
                'raster': 'https://storage.googleapis.com/ipbes-ndr-ecoshard-data/ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif',
            },
            'target_nodata': -1,
            'target_raster_path': 'masked_nathab_esa.tif',
        },
        {
            'expression': 'mask(raster, %s, invert=False)'%(str([20,30]+[x for x in range(80,127)])[1:-1]),
            'symbol_to_path_map': {
                'raster': 'https://storage.googleapis.com/ecoshard-root/working-shards/coopernicus_landcover_discrete_compressed_md5_264bda5338a02e4a6cc10412b8edad9f.tif',
            },
            'target_nodata': -1,
            'target_raster_path': 'masked_nathab_copernicus.tif',
        },
    ]
    for masker in masker_list:
       raster_calculations_core.evaluate_calculation(
            masker, TASK_GRAPH, WORKSPACE_DIR)
    

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return #terminates at this point

    potential_service_list = [
        {
            'expression': 'mask*service',
            'symbol_to_path_map': {
                'mask': 'https://storage.googleapis.com/ecoshard-root/working-shards/masked_nathab_esa_md5_40577bae3ef60519b1043bb8582a07af.tif',
                'service': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/ESACCI_LC_L4_LCCS_borrelli_sediment_deposition_md5_3e0ccb34352269d7eb688dd488de002f.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "potential_sedimentdeposition.tif",
            'build_overview': True,
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'mask*service',
            'symbol_to_path_map': {
                'mask': 'https://storage.googleapis.com/ecoshard-root/working-shards/masked_nathab_esa_md5_40577bae3ef60519b1043bb8582a07af.tif',
                'service': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_esa_2015_n_retention_md5_d10a396b8f0fee70dd3bbd3524a6a97c.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "potential_nitrogenretention.tif",
            'build_overview': True,
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
    ]

    for calculation in potential_service_list:
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
    ]

    for calculation in derived_raster_calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()


    # just build overviews
    raster_calculation_list = [
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': '',
            },
            'target_nodata': -1,
            'target_raster_path': "potential_pollination.tif",
            'build_overview': True,
        },
    ]

    for calculation in raster_calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)


    #calculate people fed equivalents from individual nutrient data
    raster_calculation_list = [
        {
            'expression': '(va/486980 + en/3319921 + fo/132654) / 3',
            'symbol_to_path_map': {
                'va': '../pollination_esa_tifs/prod_poll_dep_realized_va_10s_ESACCI_LC_L4_LCSS.tif',
                'en': '../pollination_esa_tifs/prod_poll_dep_realized_en_10s_ESACCI_LC_L4_LCSS.tif',
                'fo': '../pollination_esa_tifs/prod_poll_dep_realized_fo_10s_ESACCI_LC_L4_LCSS.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "pollination_ppl_fed_on_ag_10s_esa.tif",
            'build_overview': True,
        },
    ]

    for calculation in raster_calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    


if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(WORKSPACE_DIR, NCPUS, 5.0)
    main()
