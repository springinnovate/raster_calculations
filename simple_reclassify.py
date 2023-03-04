"""Change one value to another for all pixels in a raster."""
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


logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


def simple_reclass_op(original_value, target_value):
    def _simple_reclass_op(array):
        result = array.copy()
        result[array==original_value] = target_value
        return result
    return _simple_reclass_op


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=(
        'Reclassify the value in a raster to another value for all pixels'))
    parser.add_argument('raster_path', help='Path to the base raster.')
    parser.add_argument('original_value', type=float, help=(
        'The value in `raster_path` to be converted.'))
    parser.add_argument(
        'target_value', type=float, help=(
            'The value to convert `original_value` to in `raster_path`.'))
    parser.add_argument(
        'target_raster_path', help='Path to the raster to create.')
    args = parser.parse_args()

    if os.path.exists(args.target_raster_path):
        raise ValueError(
            f'{args.target_raster_path} already exists, will not overwrite.')

    raster_info = geoprocessing.get_raster_info(args.raster_path)

    # count valid pixels
    geoprocessing.raster_calculator(
        [(args.raster_path, 1)],
        simple_reclass_op(args.original_value, args.target_value),
        args.target_raster_path, raster_info['datatype'],
        raster_info['nodata'][0])
