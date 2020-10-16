"""Demo of optimization."""
import logging
import os
import sys

import pygeoprocessing

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)

LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.WARN)

if __name__ == '__main__':
    path_to_raster = 'Total_C_v10_2km.tif'
    output_directory = 'raster_optimization_output'
    churn_dir = os.path.join(output_directory, 'churn')
    try:
        os.makedirs(churn_dir)
    except OSError:
        pass
    
    pygeoprocessing.raster_optimization(
       [(path_to_raster, 1)], churn_dir, output_directory,
       goal_met_cutoffs=[x/100 for x in range(5, 91, 5)],
       heap_buffer_size=2**26)

