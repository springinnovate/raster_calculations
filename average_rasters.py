"""Average arbitrary set of rasters."""
import argparse
import glob
import logging
import os
import shutil
import sys
import tempfile

from ecoshard import geoprocessing
from osgeo import gdal
import ecoshard
import numpy


TARGET_AVERAGE_RASTER_PATH = 'average_raster.tif'
TARGET_VALID_COUNT_RASTER_PATH = 'valid_count_raster.tif'

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)

COUNT_NODATA = -1
AVERAGE_NODATA = -9999


def count_op(*value_nodata_list):
    """Calculate count of valid pixels over each pixel stack."""
    result = numpy.zeros(value_nodata_list[0].shape, dtype=numpy.int32)
    valid_mask = numpy.zeros(result.shape, dtype=numpy.bool)
    list_len = len(value_nodata_list)
    for array, nodata in zip(
            value_nodata_list[0:list_len//2],
            value_nodata_list[list_len//2::]):
        local_valid_mask = ~numpy.isclose(array, nodata)
        result[local_valid_mask] += 1
        valid_mask |= local_valid_mask
    result[~valid_mask] = COUNT_NODATA
    return result


def average_op(*value_nodata_list):
    """Calculate count of valid pixels over each pixel stack."""
    result = numpy.zeros(value_nodata_list[0].shape, dtype=numpy.float32)
    count = numpy.zeros(value_nodata_list[0].shape, dtype=numpy.float32)
    valid_mask = numpy.zeros(result.shape, dtype=numpy.bool)
    list_len = len(value_nodata_list)
    for array, nodata in zip(
            value_nodata_list[0:list_len//2],
            value_nodata_list[list_len//2::]):
        local_valid_mask = ~numpy.isclose(array, nodata)
        count[local_valid_mask] += 1
        result[local_valid_mask] += array[local_valid_mask]
        valid_mask |= local_valid_mask
    result[valid_mask] /= count[valid_mask]
    result[~valid_mask] = AVERAGE_NODATA
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Raster averager.')
    parser.add_argument(
        'raster_pattern', nargs='+', help='List of rasters to average.')
    parser.add_argument(
        '--prefix', default='', help='Prefix to add to output file.')
    args = parser.parse_args()

    working_dir = tempfile.mkdtemp(dir='.', prefix='avg_raster_workspace')

    file_list = [
        path for pattern in args.raster_pattern for path in glob.glob(pattern)]

    aligned_list = [
        os.path.join(working_dir, os.path.basename(path))
        for path in file_list]

    target_pixel_size = geoprocessing.get_raster_info(
        file_list[0])['pixel_size']

    geoprocessing.align_and_resize_raster_stack(
        file_list, aligned_list, ['near'] * len(aligned_list),
        target_pixel_size, 'union')

    nodata_list = [
        (geoprocessing.get_raster_info(path)['nodata'][0], 'raw')
        for path in aligned_list]

    # count valid pixels
    geoprocessing.raster_calculator(
        [(path, 1) for path in aligned_list] + nodata_list, count_op,
        args.prefix+TARGET_VALID_COUNT_RASTER_PATH, gdal.GDT_Int32,
        COUNT_NODATA)

    # average valid pixels
    geoprocessing.raster_calculator(
        [(path, 1) for path in aligned_list] + nodata_list, average_op,
        args.prefix+TARGET_AVERAGE_RASTER_PATH, gdal.GDT_Float32,
        AVERAGE_NODATA)

    ecoshard.build_overviews(
        args.prefix+TARGET_AVERAGE_RASTER_PATH)

    ecoshard.build_overviews(
        args.prefix+TARGET_VALID_COUNT_RASTER_PATH)

    shutil.rmtree(working_dir)
