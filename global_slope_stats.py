"""Global slope stats."""
import glob
import os
import logging
import sys
import zipfile

import ecoshard
from ecoshard import taskgraph

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


def download_and_unzip(url, target_dir, target_token_path=None):
    """Download `url` to `target_dir` and touch `target_token_path`.

    Parameters:
        url (str): url to file to download
        target_dir (str): path to a local directory to download and unzip the
            file to. The contents will be unzipped into the same directory as
            the zipfile.
        target_token_path (str): If not None, a path a file to touch when
            the unzip is complete. This parameter is added to work well with
            the ecoshard library that expects a file to be created after
            an operation is complete. It may be complicated to list the files
            that are unzipped, so instead this file is created and contains
            the timestamp of when this function completed.

    Returns:
        None.

    """
    zipfile_path = os.path.join(target_dir, os.path.basename(url))
    LOGGER.info('download %s, to: %s', url, zipfile_path)
    ecoshard.download_url(url, zipfile_path)

    LOGGER.info('unzip %s', zipfile_path)
    with zipfile.ZipFile(zipfile_path, 'r') as zip_ref:
        zip_ref.extractall(target_dir)

    if target_token_path:
        with open(target_token_path, 'w') as touchfile:
            touchfile.write(f'unzipped {zipfile_path}')
    LOGGER.info('download an unzip for %s complete', zipfile_path)


def main():
    """Main."""
    dem_dir = os.path.join(CHURN_DIR, 'dem_dir')
    for dir_path in [WORKSPACE_DIR, CHURN_DIR, ECOSHARD_DIR, dem_dir]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    task_graph = taskgraph.TaskGraph(CHURN_DIR, -1)
    download_task = task_graph.add_task(
        func=download_and_unzip,
        args=(GLOBAL_DEM_ECOSHARD_URL, dem_dir),
        task_name='unzip and download dem')
    download_task.join()

    for dem_tif in glob.glob(os.path.join(dem_dir, '*.tif')):
        LOGGER.debug(dem_tif)

    task_graph.join()
    task_graph.close()


if __name__ == '__main__':
    main()
