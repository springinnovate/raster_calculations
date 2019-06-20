"""Entry point for raster stats."""
import time
import itertools
import os
import tempfile
import struct
import argparse
import sys
import logging
import heapq
import math

import pygeoprocessing
import numpy
from osgeo import gdal

LOGGER = logging.getLogger(__name__)

_BLOCK_SIZE = 2**20
_LARGEST_STRUCT_PACK = 1024

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


def calculate_raster_stats(raster_path, output_csv_path, percentiles=None, ):
    """Calculate raster stats.

    Parameters:
        raster_path (str): path to raster.
        percentile (list): list of desired percentiles.

    Returns:
        dict of raster stats
            -max, min, mean, user defined percentiles, mode, stdev, percent
             nodata.
            -if int, table & count of values.

    """
    info_callback = _make_logger_callback(
        "Warp %.1f%% complete %s")
    output_csv_file = open(output_csv_path, 'w')
    output_csv_file.write('raster path,min,max,mean,stdev,%s\n' % ','.join([
        str(x) for x in sorted(percentiles)]))
    raster = gdal.OpenEx(raster_path, gdal.OF_RASTER)
    band = raster.GetRasterBand(1)
    band.ComputeStatistics(0, info_callback, None)
    min_val, max_val, mean, stdev = band.GetStatistics(0, 0)
    info_string = '\nRaster stats:\n*************\n'
    info_string += '  min: %s\n' % min_val
    info_string += '  max: %s\n' % max_val
    info_string += ' mean: %s\n' % mean
    info_string += 'stdev: %s\n' % stdev
    output_csv_file.write('%s,%s,%s,%s,%s' % (
        raster_path, min_val, max_val, mean, stdev))
    LOGGER.info("intermediate result for %s:\n%s", raster_path, info_string)
    if percentiles:
        total_valid = 0
        nodata = band.GetNoDataValue()
        band = None
        raster = None
        LOGGER.debug("calculating number of non-nodata pixels")
        for _, data_block in pygeoprocessing.iterblocks((raster_path, 1)):
            total_valid += numpy.count_nonzero(
                ~numpy.isclose(data_block, nodata))
        sorted_raster_iterator = _sort_to_disk(raster_path)
        current_offset = 0
        LOGGER.info("calculating percentiles")
        for percentile in sorted(percentiles):
            LOGGER.info("calculating percentile %d", percentile)
            skip_size = int(
                (int(percentile)/100.0*total_valid) - current_offset)
            current_offset += skip_size+1
            if current_offset >= total_valid:
                skip_size -= current_offset-total_valid+2
            percentile_value = next(itertools.islice(
                sorted_raster_iterator, skip_size, skip_size+1))
            info_string += '%3dth percentile: %s\n' % (
                percentile, percentile_value)
            output_csv_file.write(',%s' % percentile_value)
            output_csv_file.flush()
    output_csv_file.close()
    LOGGER.info(info_string)


def _sort_to_disk(dataset_path):
    """Return an iterable of non-nodata pixels in sorted order.

    Parameters:
        dataset_path (string): a path to a floating point GDAL dataset

    Returns:
        an iterable that produces value in decreasing sorted order

    """
    def _read_score_index_from_disk(score_file_path, datatype_id):
        """Create generator of float/int value from the given filenames.

        Reads a buffer of `buffer_size` big before to avoid keeping the
        file open between generations.

        score_file_path (string): a path to a file that has 32 bit data
            packed consecutively
        datatype_id (str): either 'i' or 'f' indicating integer or floating
            point types.

        Yields:
            next (score, index) tuple in the given score and index files.

        """
        try:
            score_buffer = ''
            file_offset = 0
            buffer_offset = 0  # initialize to 0 to trigger the first load

            # ensure buffer size that is not a perfect multiple of 4
            read_buffer_size = int(math.sqrt(_BLOCK_SIZE))
            read_buffer_size = read_buffer_size - read_buffer_size % 4

            while True:
                if buffer_offset == len(score_buffer):
                    score_file = open(score_file_path, 'rb')
                    score_file.seek(file_offset)

                    score_buffer = score_file.read(read_buffer_size)
                    score_file.close()

                    file_offset += read_buffer_size
                    buffer_offset = 0
                packed_score = score_buffer[buffer_offset:buffer_offset+4]
                buffer_offset += 4
                if not packed_score:
                    break
                yield struct.unpack(datatype_id, packed_score)[0]
        finally:
            # deletes the files when generator goes out of scope or ends
            os.remove(score_file_path)

    def _sort_cache_to_iterator(score_cache, datatype_id):
        """Flushes the current cache to a heap and return it.

        Parameters:
            score_cache (1d numpy.array): contains score pixels
            datatype_id (str): either 'i' or 'f' to indicate integer or
                float for struct formatting.

        Returns:
            Iterable to visit scores/indexes in increasing score order.

        """
        # sort the whole bunch to disk
        score_file = tempfile.NamedTemporaryFile(delete=False)

        sort_index = score_cache.argsort()
        score_cache = score_cache[sort_index]
        for index in range(0, score_cache.size, _LARGEST_STRUCT_PACK):
            score_block = score_cache[index:index+_LARGEST_STRUCT_PACK]
            score_file.write(struct.pack(
                '%s%s' % (score_block.size, datatype_id), *score_block))

        score_file_path = score_file.name
        score_file.close()

        return _read_score_index_from_disk(score_file_path, datatype_id)

    dataset_info = pygeoprocessing.get_raster_info(dataset_path)
    nodata = dataset_info['nodata'][0]

    # This will be a list of file iterators we'll pass to heap.merge
    iters = []

    if dataset_info['datatype'] in (
            gdal.GDT_Byte, gdal.GDT_Int16, gdal.GDT_Int32):
        datatype_id = 'i'
    elif dataset_info['datatype'] in (gdal.GDT_Float32,):
        datatype_id = 'f'
    else:
        raise ValueError("can't process stats for this raster type")
    for scores_data, scores_block in pygeoprocessing.iterblocks(
            (dataset_path, 1), largest_block=_BLOCK_SIZE):
        # flatten and scale the results
        scores_block = scores_block.flatten()

        col_coords, row_coords = numpy.meshgrid(
            range(scores_data['xoff'], scores_data['xoff'] +
                  scores_data['win_xsize']),
            range(scores_data['yoff'], scores_data['yoff'] +
                  scores_data['win_ysize']))

        sort_index = scores_block.argsort()
        sorted_scores = scores_block[sort_index]

        # search for nodata values are so we can splice them out
        left_index = numpy.searchsorted(sorted_scores, nodata, side='left')
        right_index = numpy.searchsorted(
            sorted_scores, nodata, side='right')

        # remove nodata values
        score_cache = numpy.concatenate(
            (sorted_scores[0:left_index], sorted_scores[right_index::]))
        iters.append(_sort_cache_to_iterator(score_cache, datatype_id))

    return heapq.merge(*iters)


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
    parser = argparse.ArgumentParser(description='raster stats.')
    parser.add_argument(
        'filepath', help='Raster file to get stats.')
    parser.add_argument(
        '--percentiles', nargs='*', help='list of percentiles to calculate',
        type=int, default=None)
    parser.add_argument(
        '-o', '--output_csv', help='path to output CSV file', type=str)
    args = parser.parse_args()
    raster_stats = calculate_raster_stats(
        args.filepath, args.output_csv, percentiles=args.percentiles)
    LOGGER.debug(raster_stats)
