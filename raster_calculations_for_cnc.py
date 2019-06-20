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

    # Becky, here's an example of how to use mask:


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
    ]
    for masker in masker_list:
       raster_calculations_core.evaluate_calculation(
            masker, TASK_GRAPH, WORKSPACE_DIR)
    TASK_GRAPH.join()

    potential_service_list = [
        {
            'expression': 'mask*service',
            'symbol_to_path_map': {
                'mask': 'masked_nathab_esa.tif',
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
                'mask': 'masked_nathab_esa.tif',
                'service': 'https://storage.googleapis.com/ipbes-natcap-ecoshard-data-for-publication/worldclim_esa_2015_n_retention_md5_d10a396b8f0fee70dd3bbd3524a6a97c.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "potential_nitrogenretention.tif",
            'build_overview': True,
        },
    ]

    for calculation in potential_service_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()

    TASK_GRAPH.close()

    return #terminates at this point

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
    ]



    for calculation in derived_raster_calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()


if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(WORKSPACE_DIR, NCPUS, 5.0)
    main()
