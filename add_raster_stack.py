"""Calculate (Raster A - Raster B) but treat nodata as 0'."""
import argparse
import logging
import glob
import shutil

from ecoshard import geoprocessing
from ecoshard.geoprocessing.geoprocessing import _GDAL_TYPE_TO_NUMPY_LOOKUP
from osgeo import gdal
import numpy

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(__name__)


def _add_with_0(nodata_list, target_nodata):
    def __add_with_0(*array_list):
        """Sum the monthly qfis."""
        running_sum = numpy.zeros(array_list[0].shape)
        running_valid_mask = numpy.zeros(running_sum.shape, dtype=bool)
        for local_array, local_nodata in zip(array_list, nodata_list):
            if local_nodata is not None:
                local_valid_mask = ~numpy.isclose(local_array, local_nodata)
            else:
                local_valid_mask = numpy.ones(local_array.shape, dtype=bool)
            running_sum[local_valid_mask] += local_array[local_valid_mask]
            running_valid_mask |= local_valid_mask
        running_sum[~running_valid_mask] = target_nodata
        return running_sum


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Add all rasters together and allow for nodata holes')
    parser.add_argument(
        'raster_path_pattern', nargs='+', help='list of rasters or patterns')
    parser.add_argument(
        '--target_nodata', type=float, help='set the target nodata')
    parser.add_argument(
        '--target_path', help=(
            'Path to target file, if not defined create unique name in '
            'current directory.'))
    args = parser.parse_args()

    raster_path_band_list = [
        (path, 1)
        for raster_path_pattern in args.raster_path_pattern
        for path in glob.glob(raster_path_pattern)]

    nodata_list = [
        geoprocessing.get_raster_info(path[0])['nodata'][0]
        for path in raster_path_band_list]

    LOGGER.info('doing addition')
    geoprocessing.raster_calculator(
        raster_path_band_list, _add_with_0(nodata_list, args.target_nodata),
        args.target_path, gdal.GDT_Float32, args.target_nodata)

    LOGGER.info('removing workspace dir')
    shutil.rmtree(args.working_dir, ignore_errors=True)
