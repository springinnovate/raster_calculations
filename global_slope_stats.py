"""Global slope stats."""
import glob
import os
import logging
import sys

import ecoshard
import taskgraph

GLOBAL_DEM_ECOSHARD_URL = (
    'https://storage.googleapis.com/nci-ecoshards/'
    'global_dem_3s_blake2b_0532bf0a1bedbe5a98d1dc449a33ef0c.zip')

WORKSPACE_DIR = 'global_slope_workspace'
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshard')

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(processName)s %(levelname)s '
        '%(name)s [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


def main():
    """Main."""
    task_graph = taskgraph.TaskGraph(CHURN_DIR, -1)
    dem_dir = os.path.join(CHURN_DIR, 'dem_dir')
    download_task = task_graph.add_task(
        func=ecoshard.download_and_unzip,
        args=(GLOBAL_DEM_ECOSHARD_URL, dem_dir),
        task_name='unzip and download dem')
    download_task.join()

    for dem_tif in glob.glob(os.path.join(dem_dir, '*.tif')):
        LOGGER.debug(dem_tif)

    task_graph.join()
    task_graph.close()


if __name__ == '__main__':
    main()
