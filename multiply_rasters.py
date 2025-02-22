"""See python [scriptname] --help"""
import os
import shutil
import argparse
import logging
import sys
import tempfile

from ecoshard import geoprocessing
import numpy

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


def mult_op(a_nodata, b_nodata, target_nodata):
    def _mult_op(array_a, array_b):
        result = numpy.full(array_a.shape, target_nodata)
        valid_mask = numpy.ones(array_a.shape, dtype=bool)
        if a_nodata is not None:
            valid_mask &= array_a != a_nodata
        valid_mask &= numpy.isfinite(array_a)
        if b_nodata is not None:
            valid_mask &= array_b != b_nodata
        valid_mask &= numpy.isfinite(array_b)
        result[valid_mask] = array_a[valid_mask]*array_b[valid_mask]
        return result
    return _mult_op


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(description=(
        'Multiply two rasters (A*B) of equal size and projection'))
    parser.add_argument('raster_A_path', help='Path to raster A.')
    parser.add_argument('raster_B_path', help='Path to raster B.')
    parser.add_argument('target_raster_path', help='Path to target raster')
    parser.add_argument('--target_nodata', type=float, help=(
        'If set, use this nodata, otherwise use target nodata value from '
        'raster A'))
    args = parser.parse_args()

    base_raster_list = [
        args.raster_A_path,
        args.raster_B_path]
    working_dir = tempfile.mkdtemp(
        dir=os.path.dirname(args.target_raster_path))
    target_raster_path_list = [
        os.path.join(working_dir, os.path.basename(path))
        for path in base_raster_list]
    pixel_size = geoprocessing.get_raster_info(
        args.raster_A_path)['pixel_size']
    geoprocessing.align_and_resize_raster_stack(
        base_raster_list, target_raster_path_list, ['near']*2,
        pixel_size, 'intersection')

    raster_info = geoprocessing.get_raster_info(args.raster_A_path)

    a_nodata = geoprocessing.get_raster_info(target_raster_path_list[0])['nodata'][0]
    b_nodata = geoprocessing.get_raster_info(target_raster_path_list[1])['nodata'][0]
    target_nodata = args.target_nodata
    if target_nodata is None:
        target_nodata = a_nodata

    geoprocessing.raster_calculator(
        [(path, 1) for path in target_raster_path_list],
        mult_op(a_nodata, b_nodata, target_nodata),
        args.target_raster_path, raster_info['datatype'], target_nodata)
    shutil.rmtree(working_dir)

if __name__ == '__main__':
    main()
