"""Compress a directory of GeoTiffs."""
import argparse
import time
import sys
import os
import logging

import taskgraph
import pygeoprocessing
import gdal

logging.basicConfig(
    level=logging.INFO,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)

LOGGER = logging.getLogger(__name__)


def compress_to(task_graph, base_raster_path, resample_method, target_path):
    gtiff_driver = gdal.GetDriverByName('GTiff')
    base_raster = gdal.OpenEx(base_raster_path, gdal.OF_RASTER)
    LOGGER.info('compress %s to %s' % (base_raster_path, target_path))
    compressed_raster = gtiff_driver.CreateCopy(
        target_path, base_raster, options=(
            'TILED=YES', 'BIGTIFF=YES', 'COMPRESS=LZW', 'BLOCKXSIZE=256',
            'BLOCKYSIZE=256'))

    min_dimension = min(
        pygeoprocessing.get_raster_info(target_path)['raster_size'])
    LOGGER.info(f"min min_dimension {min_dimension}")

    overview_levels = []
    current_level = 2
    while True:
        if min_dimension // current_level == 0:
            break
        overview_levels.append(current_level)
        current_level *= 2
    LOGGER.info(f'level list: {overview_levels}')
    compressed_raster.BuildOverviews(
        resample_method, overview_levels, callback=_make_logger_callback(
            f'build overview for {os.path.basename(target_path)} '
            '%.2f%% complete'))


def main():
    """Entry point, takes in base path and compression algorithm."""
    task_graph = taskgraph.TaskGraph('compression_taskgraph_dir', -1)
    parser = argparse.ArgumentParser(
        description='Compress and build overview for raster.')
    parser.add_argument(
        'filepath', nargs='+', help='Files to hash and rename.')
    parser.add_argument(
        '--resample_method', default='near',
        help='A gdal valid interpolation method (e.g. near, bilinear, etc.')
    args = parser.parse_args()
    for file_path in args.filepath:
        target_path = f'{os.path.splitext(file_path)[0]}_compressed.tif'
        LOGGER.info(f'starting {file_path} to {target_path}')
        compress_to(task_graph, file_path, args.resample_method, target_path)


def _make_logger_callback(message):
    """Build a timed logger callback that prints ``message`` replaced.

    Parameters:
        message (string): a string that expects 2 placement %% variables,
            first for % complete from ``df_complete``, second from
            ``p_progress_arg[0]``.

    Returns:
        Function with signature:
            logger_callback(df_complete, psz_message, p_progress_arg)

    """
    def logger_callback(df_complete, _, p_progress_arg):
        """Argument names come from the GDAL API for callbacks."""
        try:
            current_time = time.time()
            if ((current_time - logger_callback.last_time) > 5.0 or
                    (df_complete == 1.0 and
                     logger_callback.total_time >= 5.0)):
                # In some multiprocess applications I was encountering a
                # ``p_progress_arg`` of None. This is unexpected and I suspect
                # was an issue for some kind of GDAL race condition. So I'm
                # guarding against it here and reporting an appropriate log
                # if it occurs.
                if p_progress_arg:
                    LOGGER.info(message, df_complete * 100, p_progress_arg[0])
                else:
                    LOGGER.info(
                        'p_progress_arg is None df_complete: %s, message: %s',
                        df_complete, message)
                logger_callback.last_time = current_time
                logger_callback.total_time += current_time
        except AttributeError:
            logger_callback.last_time = time.time()
            logger_callback.total_time = 0.0

    return logger_callback


if __name__ == '__main__':
    main()
