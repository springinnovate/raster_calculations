"""These calculations are for sharing with others."""

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
import numpy
from ecoshard import taskgraph
import ecoshard.geoprocessing as pygeoprocessing

gdal.SetCacheMax(2**25)

WORKSPACE_DIR = 'scratch_workspace'
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
        #{
        #    'expression': 'raster1>=1',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\repositories\spring\raster_calculations\ncp1_climate2_overlap3_WARPED_near_md5_a0a5579fe5425ba45b6b69ca0622516d.tif",
        #    },
        #    'target_nodata': -1,
        #    'target_raster_path': r"D:\repositories\spring\raster_calculations\Roadmap2030\NCP_mask.tif",
        #},
        #{
        #    'expression': 'raster2>=5',
        #    'symbol_to_path_map': {
        #        'raster2': r"D:\repositories\spring\raster_calculations\Roadmap2030\CountryMaxClassifiedWithZeros_aligned_to_cna.tif",
        #    },
        #    'target_nodata': -1,
        #    'target_raster_path': r"D:\repositories\spring\raster_calculations\Roadmap2030\DPI_mask.tif",
        #},
        #{
        #    'expression': 'raster1 + (raster2*2)',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\repositories\spring\raster_calculations\Roadmap2030\NCP_mask.tif",
        #        'raster2': r"D:\repositories\spring\raster_calculations\Roadmap2030\DPI_mask.tif",
        #    },
        #    'target_nodata': -1,
        #    'target_raster_path': r"D:\repositories\spring\raster_calculations\Roadmap2030\NCP1_DPI2_overlap3.tif",
        #},
        #{
        #    'expression': 'raster1<=30',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\repositories\spring\raster_calculations\Roadmap2030\minshort_speciestarget_esh10km_repruns10_ranked_aligned.tif",
        #    },
        #    'target_nodata': -9999, =-3.4e+38
        #    'target_raster_path': r"D:\repositories\spring\raster_calculations\Roadmap2030\biodiversity_mask.tif",
        #},
        {
            'expression': 'raster1>=0.25',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\spring\raster_calculations\Roadmap2030\csi_future_ssp126_aligned.tif",
            },
            'target_nodata': float(numpy.finfo(numpy.float32).min),
            'target_raster_path': r"D:\repositories\spring\raster_calculations\Roadmap2030\csi126_mask.tif",
        },
        {
            'expression': 'raster1>=0.25',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\spring\raster_calculations\Roadmap2030\csi_future_ssp245_aligned.tif",
            },
            'target_nodata': float(numpy.finfo(numpy.float32).min),
            'target_raster_path': r"D:\repositories\spring\raster_calculations\Roadmap2030\csi245_mask.tif",
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
