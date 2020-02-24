"""Mask global rasters to have integer bins rather than values."""
import glob
import logging
import os
import re
import sys

from osgeo import gdal
import pandas
import pygeoprocessing
import numpy

PERCENTILES = [
    0, 0.01, 1, 2, 3, 4, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70,
    75, 80, 85, 90, 95, 96, 97, 98, 99, 99.9, 100]

TENTH_INDEXES = [PERCENTILES.index(x) for x in range(10, 101, 10)]

BIN_INTEGERS = [1 + int(x) // 10 for x in PERCENTILES]

BIN_WORKSPACE = 'bin_raster_paths'
try:
    os.makedirs(BIN_WORKSPACE)
except OSError:
    pass

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


def mask_to_percentile(
        base_array, base_nodata, target_nodata, cutoff_percentile_values):
    result = numpy.zeros(base_array.shape, dtype=numpy.int8)
    zero_mask = base_array == 0.0
    nodata_mask = numpy.isclose(base_array, base_nodata)
    result[nodata_mask] = target_nodata
    for cutoff_index in range(len(cutoff_percentile_values)-1):
        bin_value = cutoff_index + 1
        bin_mask = ~zero_mask & (
            (base_array >= cutoff_percentile_values[cutoff_index]) &
            (base_array < cutoff_percentile_values[cutoff_index+1]))
        result[bin_mask] = bin_value
    return result


if __name__ == '__main__':
    for raster_path in glob.glob('realized_service/*.tif'):
        dirname = os.path.dirname(raster_path)
        basename = os.path.basename(raster_path)
        base_without_hash = re.match('(.*)_md5*', basename).group(1)
        print(dirname, base_without_hash)
        for table_path in glob.glob(os.path.join(
            dirname, 'tables', '%s*.csv' % os.path.splitext(
                base_without_hash)[0])):
            frame = pandas.read_csv(table_path)
            cutoff_percentile_values = []
            for index, tenth_index in enumerate(TENTH_INDEXES):
                cutoff_percentile_values.append(frame.iloc[0, 2+tenth_index])
            break
        print(cutoff_percentile_values)
        bin_raster_path = os.path.join(
            BIN_WORKSPACE, '%s_bins.tif' % os.path.splitext(
                os.path.basename(raster_path))[0])
        nodata = pygeoprocessing.get_raster_info(raster_path)['nodata'][0]
        print(bin_raster_path)
        pygeoprocessing.raster_calculator(
            [(raster_path, 1), (nodata, 'raw'), (11, 'raw'),
             (cutoff_percentile_values, 'raw')], mask_to_percentile,
            bin_raster_path, gdal.GDT_Byte, 11)
    print('all done!')
