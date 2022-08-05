"""Calculate (Raster A - Raster B) but treat nodata as 0'."""
import argparse
import logging
import os
import shutil

from ecoshard import geoprocessing
import numpy

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(__name__)


def _add_with_0(array_a, array_b, a_nodata, b_nodata):
    if a_nodata is not None:
        result = numpy.full(array_a.shape, a_nodata)
        a_valid_mask = (array_a != a_nodata)
    else:
        result = numpy.copy(array_a)
        a_valid_mask = numpy.full(array_a.shape, True)

    if b_nodata is not None:
        b_valid_mask = (array_b != b_nodata)
    else:
        b_valid_mask = numpy.full(array_b.shape, True)

    valid_mask = (a_valid_mask & b_valid_mask)
    result[valid_mask] = array_a[valid_mask] + array_b[valid_mask]

    missing_a_mask = ~a_valid_mask & b_valid_mask
    result[missing_a_mask] = array_b[missing_a_mask]
    missing_b_mask = a_valid_mask & ~b_valid_mask
    result[missing_b_mask] = array_a[missing_b_mask]
    return result


def _sub_with_0(array_a, array_b, a_nodata, b_nodata):
    if a_nodata is not None:
        result = numpy.full(array_a.shape, a_nodata)
        a_valid_mask = (array_a != a_nodata)
    else:
        result = numpy.copy(array_a)
        a_valid_mask = numpy.full(array_a.shape, True)

    if b_nodata is not None:
        b_valid_mask = (array_b != b_nodata)
    else:
        b_valid_mask = numpy.full(array_b.shape, True)

    valid_mask = (a_valid_mask & b_valid_mask)
    result[valid_mask] = array_a[valid_mask] - array_b[valid_mask]

    missing_a_mask = ~a_valid_mask & b_valid_mask
    result[missing_a_mask] = -array_b[missing_a_mask]
    missing_b_mask = a_valid_mask & ~b_valid_mask
    result[missing_b_mask] = array_a[missing_b_mask]
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='calculate (Raster A - Raster B) but treat nodata as 0')
    parser.add_argument('raster_a_path', help='Path to raster a.')
    parser.add_argument('raster_b_path', help='Path to raster a.')
    parser.add_argument(
        '--working_dir', default='subtract_missing_as_0_workspace',
        help='location to store temporary files')
    parser.add_argument(
        '--add', action='store_true', help='select if you want to add')
    parser.add_argument(
        '--subtract', action='store_true',
        help='select if you want to subtract')
    args = parser.parse_args()

    if args.add and args.subtract:
        raise ValueError('choose only add or subtract not both')
    if not args.add and not args.subtract:
        raise ValueError('choose --add or --subtract')

    if args.add:
        _func = _add_with_0
        sep = ' + '
    if args.subtract:
        _func = _sub_with_0
        sep = ' - '

    raster_a_info = geoprocessing.get_raster_info(args.raster_a_path)
    raster_b_info = geoprocessing.get_raster_info(args.raster_b_path)

    raster_list = [args.raster_a_path, args.raster_b_path]

    if any(numpy.array(raster_a_info['raster_size']) !=
            numpy.array(raster_b_info['raster_size'])):
        LOGGER.info(
            f'{args.raster_a_path} and {args.raster_b_path} have different '
            f'sizes, doing an alignment before subtraction')
        os.makedirs(args.working_dir, exist_ok=True)
        aligned_raster_list = [
            os.path.join(args.working_dir, os.path.basename(path))
            for path in raster_list]
        geoprocessing.align_and_resize_raster_stack(
            raster_list, aligned_raster_list, ['near', 'near'],
            raster_a_info['pixel_size'], 'union',
            target_projection_wkt=raster_a_info['projection_wkt'])
        raster_list = aligned_raster_list

    target_path = (
        f'{os.path.splitext(os.path.basename(args.raster_a_path))[0]} '
        f' {sep} '
        f'{os.path.splitext(os.path.basename(args.raster_b_path))[0]}.tif')

    LOGGER.info('doing subtraction')
    geoprocessing.raster_calculator(
        [(p, 1) for p in raster_list] +
        [(raster_a_info['nodata'][0], 'raw'),
         (raster_b_info['nodata'][0], 'raw')], _func, target_path,
        raster_a_info['datatype'], raster_a_info['nodata'][0])

    LOGGER.info('removing workspace dir')
    shutil.rmtree(args.working_dir, ignore_errors=True)
