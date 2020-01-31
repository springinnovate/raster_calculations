"""Average arbitrary set of rasters."""
import argparse
import glob
import logging
import os
import shutil
import sys
import tempfile

from osgeo import gdal
import numpy
import pygeoprocessing


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


def count_op(*value_nodata_list):
    """Calculate count of valid pixels over each pixel stack."""
    result = numpy.zeros(value_nodata_list[0].shape, dtype=numpy.int32)
    result[:] = COUNT_NODATA
    nodata_mask = numpy.ones(result.shape, dtype=numpy.bool)
    for array, nodata in zip(
            value_nodata_list[0::-1], value_nodata_list[1::-1]):
        local_nodata_mask = numpy.isclose(array, nodata)
        result[~local_nodata_mask] += 1
        nodata_mask &= local_nodata_mask
    result[nodata_mask] = COUNT_NODATA
    return result


def average_op(*value_nodata_list):
    """Calculate count of valid pixels over each pixel stack."""
    result = numpy.zeros(value_nodata_list[0].shape, dtype=numpy.float32)
    count = numpy.zeros(value_nodata_list[0].shape, dtype=numpy.int32)

    result[:] = COUNT_NODATA
    nodata_mask = numpy.ones(result.shape, dtype=numpy.bool)
    for array, nodata in zip(
            value_nodata_list[0::-1], value_nodata_list[1::-1]):
        local_nodata_mask = numpy.isclose(array, nodata)
        count[~local_nodata_mask] += 1
        result[~local_nodata_mask] += array[~local_nodata_mask]
        nodata_mask &= local_nodata_mask
    result[~nodata_mask] /= count[~nodata_mask]
    result[nodata_mask] = AVERAGE_NODATA
    return result



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Global CV analysis')
    parser.add_argument(
        'raster_pattern', nargs='+', help='List of rasters to average.')
    args = parser.parse_args()

    working_dir = tempfile.mkdtemp(prefix='avg_raster_workspace')

    file_list = [
        path for pattern in args.raster_pattern for path in glob.glob(pattern)]

    aligned_list = [os.path.join(working_dir, path) for path in file_list]

    target_pixel_size = pygeoprocessing.get_raster_info(
        file_list[0])['pixel_size']

    pygeoprocessing.align_and_resize_raster_stack(
        file_list, aligned_list, ['near'] * len(aligned_list),
        target_pixel_size, 'union')

    nodata_list = [
        (pygeoprocessing.get_raster_info(path)['nodata'][0], 'raw')
        for path in file_list]

    # count valid pixels
    pygeoprocessing.raster_calculator(
        [(path, 1) for path in aligned_list] + nodata_list, count_op,
        TARGET_VALID_COUNT_RASTER_PATH, gdal.GDT_Int32, COUNT_NODATA)

    # average valid pixels
    pygeoprocessing.raster_calculator(
        [(path, 1) for path in aligned_list] + nodata_list, average_op,
        TARGET_AVERAGE_RASTER_PATH, gdal.GDT_Float32, COUNT_NODATA)

    pygeoprocessing.build_overviews(
        TARGET_AVERAGE_RASTER_PATH)

    pygeoprocessing.build_overviews(
        TARGET_VALID_COUNT_RASTER_PATH)

    shutil.rmtree(working_dir)
