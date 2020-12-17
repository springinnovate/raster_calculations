"""
Fill lat/lng raster nodata
"""
import argparse
import glob
import logging
import multiprocessing
import os
import shutil
import warnings
import threading
import time
warnings.filterwarnings('error')

from osgeo import gdal
import ecoshard
import pygeoprocessing
import numpy
import scipy.ndimage
import taskgraph

gdal.SetCacheMax(2**26)

N_CPUS = multiprocessing.cpu_count()

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(processName)s %(levelname)s '
        '%(name)s [%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.DEBUG)
logging.getLogger('pygeoprocessing').setLevel(logging.DEBUG)


def fill_by_convolution(
        base_raster_path, convolve_radius, target_filled_raster_path):
    """Clip and fill.

    Clip the base raster data to the bounding box then fill any noodata
    holes with a weighted distance convolution.

    Args:
        base_raster_path (str): path to base raster
        convolve_radius (float): maximum convolution distance kernel in
            projected units of base.
        target_filled_raster_path (str): raster created by convolution fill,
            if holes are too far from valid pixels resulting fill will be
            nonsensical, perhaps NaN.

    Return:
        None
    """
    try:
        LOGGER.info(f'filling {base_raster_path}')
        # create working directory in the same directory as the target with
        # the same name as the target file so it can't be duplicated
        # easier to spot for debugging too
        working_dir = os.path.join(
            os.path.dirname(target_filled_raster_path),
            os.path.basename(os.path.splitext(target_filled_raster_path)[0]))
        try:
            os.makedirs(working_dir)
        except OSError:
            pass

        basename = os.path.basename(target_filled_raster_path)
        base_raster_info = pygeoprocessing.get_raster_info(base_raster_path)

        # this ensures a minimum of 3 pixels in case the pixel size is too
        # chunky
        n = max(3, int(convolve_radius / base_raster_info['pixel_size'][0]))
        base = numpy.zeros((n, n))
        base[n//2, n//2] = 1
        kernel_array = scipy.ndimage.filters.gaussian_filter(base, n/3)
        kernel_raster_path = os.path.join(working_dir, f'kernel_{basename}')
        geotransform = base_raster_info['geotransform']
        pygeoprocessing.numpy_array_to_raster(
            kernel_array, None, base_raster_info['pixel_size'],
            (geotransform[0], geotransform[3]),
            base_raster_info['projection_wkt'], kernel_raster_path)

        # scrub input raster
        sanitized_base_raster_path = os.path.join(
            working_dir, f'sanitized_{basename}')
        sanitize_raster(base_raster_path, sanitized_base_raster_path)

        # mask valid
        valid_raster_path = os.path.join(
            working_dir, f'sanitized_{basename}')
        pygeoprocessing.raster_calculator(
            [(base_raster_path, 1), (base_raster_info['nodata'][0], 'raw')],
            _mask_valid_op, valid_raster_path, gdal.GDT_Byte, None)
        mask_kernel_raster_path = os.path.join(
            working_dir, f'mask_kernel_{basename}')
        geotransform = base_raster_info['geotransform']
        mask_kernel_array = numpy.copy(kernel_array)
        mask_kernel_array[:] = 1
        pygeoprocessing.numpy_array_to_raster(
            mask_kernel_array, None, base_raster_info['pixel_size'],
            (geotransform[0], geotransform[3]),
            base_raster_info['projection_wkt'], mask_kernel_raster_path)
        coverage_raster_path = os.path.join(
            working_dir, f'coverage_{basename}')
        pygeoprocessing.convolve_2d(
            (valid_raster_path, 1), (mask_kernel_raster_path, 1),
            coverage_raster_path,
            mask_nodata=False,
            target_nodata=-1,
            target_datatype=gdal.GDT_Byte,
            working_dir=working_dir)

        # this raster will be filled with the entire convolution
        backfill_raster_path = os.path.join(
            working_dir, f'backfill_{basename}')
        base_nodata = base_raster_info['nodata'][0]
        if base_nodata is None:
            target_datatype = gdal.GDT_Float64
        else:
            target_datatype = base_raster_info['datatype']
        LOGGER.info(
            f'create backfill from {sanitized_base_raster_path} to '
            f'{backfill_raster_path}')
        pygeoprocessing.convolve_2d(
            (sanitized_base_raster_path, 1), (kernel_raster_path, 1),
            backfill_raster_path, ignore_nodata_and_edges=True,
            mask_nodata=False, normalize_kernel=True,
            target_nodata=base_nodata,
            target_datatype=target_datatype,
            working_dir=working_dir)

        LOGGER.info(
            f'fill nodata of {base_raster_path} to {backfill_raster_path}')
        pygeoprocessing.raster_calculator(
            [(base_raster_path, 1),
             (backfill_raster_path, 1), (coverage_raster_path, 1),
             (base_nodata, 'raw')], _fill_nodata_op,
            target_filled_raster_path,
            base_raster_info['datatype'], base_nodata)
        shutil.rmtree(working_dir)
    except Exception:
        LOGGER.exception(
            f'error on fill by convolution {target_filled_raster_path}')
        raise


def _fill_nodata_op(base, fill, valid, nodata):
    result = numpy.copy(base)
    valid_mask = valid > 0
    if nodata is not None:
        valid_mask &= numpy.isclose(base, nodata)
    result[valid_mask] = fill[valid_mask]
    return result


def _non_finite_to_fill_op(base_array, fill):
    result = numpy.copy(base_array)
    result[~numpy.isfinite(base_array)] = fill
    return result


def _mask_valid_op(base_array, nodata):
    """Convert valid to True nodata/invalid to False."""
    if nodata is not None:
        valid_mask = ~numpy.isclose(base_array, nodata)
    else:
        valid_mask = numpy.ones(base_array.shape, dtype=numpy.bool)
    valid_mask &= numpy.isfinite(base_array)
    return valid_mask


def sanitize_raster(base_raster_path, target_raster_path):
    """Scrub base raster of any non-finite values to nodata.

    If noodata is None then scrub to 0.

    Args:
        base_raster_path (str): path to base raster
        target_raster_path (str): path to target raster

    Return:
        None.
    """
    raster_info = pygeoprocessing.get_raster_info(
        base_raster_path)
    fill_value = raster_info['nodata'][0]
    if fill_value is None:
        fill_value = 0
    pygeoprocessing.raster_calculator(
        [(base_raster_path, 1), (fill_value, 'raw')], _non_finite_to_fill_op,
        target_raster_path, raster_info['datatype'],
        raster_info['nodata'][0])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=(
        'Fill nodata holes in lat/lng raster by weighted average distancing '
        'nearby valid pixels.'))
    parser.add_argument(
        'raster_pattern', nargs='+', help='List of rasters to average.')
    parser.add_argument(
        '--radius', required=True, type=float, help=(
            'The kernel radius to use when weighted distance averaging '
            'fill values. Same units as raster.'))
    parser.add_argument(
        '--output_dir', default='filled_raster_workspace', help=(
            'directory to place filled rasters. Default is '
            '`filled_raster_workspace`.'))
    parser.add_argument(
        '--filled_raster_prefix', default='filled_', help=(
            'Prefix to put on filled raster from their original path, '
            'default is `filled_`.'))

    args = parser.parse_args()
    for raster_pattern in args.raster_pattern:
        for raster_path in glob.glob(raster_pattern):
            pre, post = os.path.splitext(os.path.basename(raster_path))
            target_fill_path = os.path.join(
                args.output_dir,
                f'{args.filled_raster_prefix}'
                f'{pre}{args.radius}{post}')
            fill_by_convolution(raster_path, args.radius, target_fill_path)
