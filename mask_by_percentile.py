"""Mask a raster to be nodata where another raster is nodata."""
import argparse
import glob
import os
import multiprocessing
import logging

import numpy
from osgeo import gdal
from ecoshard import taskgraph
from ecoshard import geoprocessing
from ecoshard.geoprocessing import VECTOR_TYPE
from ecoshard.geoprocessing import RASTER_TYPE

gdal.SetCacheMax(2**27)

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))

LOGGER = logging.getLogger(__name__)

WORKSPACE_DIR = 'mask_by_histogram_workspace'


def check_percentile(value):
    try:
        op, num, path = value.split('-')
        num = float(num)
        if op not in ['gte', 'lte']:
            raise argparse.ArgumentTypeError("Operator must be 'gte' or 'lte'")
        if not 0 <= num <= 100:
            raise argparse.ArgumentTypeError(
                "Number must be between 0 and 100")
    except ValueError:
        raise argparse.ArgumentTypeError(
            "Invalid percentile format. Must be 'gte-<number>-target_path' or "
            "'lte-<number>-target_path'")

    return value


def mask_raster_by_value(
        base_raster_path, value, gte_or_lte, target_raster_path):
    """Mask base by mask such that any nodata in mask is set to nodata in base."""
    base_info = geoprocessing.get_raster_info(base_raster_path)
    base_nodata = base_info['nodata'][0]

    def mask_op(base_array):
        result = numpy.zeros(base_array.shape)
        valid_mask = base_array != base_nodata
        if gte_or_lte == 'gte':
            valid_mask &= (base_array >= value)
        else:
            valid_mask &= (base_array <= value)
        result[valid_mask] = 1
        return result

    geoprocessing.single_thread_raster_calculator(
        [(base_raster_path, 1)], mask_op,
        target_raster_path, gdal.GDT_Byte, None)


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(description=(
        'Mask raster by another raster. Input data is set to nodata where mask '
        'is nodata. Results are in a subdirectory called '
        f'"{WORKSPACE_DIR}".'))
    parser.add_argument(
        'input_raster_path', help='Path to raster to mask.')
    parser.add_argument(
        'percentile_to_path', nargs='+', type=check_percentile, help=(
            "Percentile in the form of 'gte-<number>-target_path' or "
            "'lte-<number>-target_path'"))
    parser.add_argument(
        '--mask_only', action='store_true', help=(
            'If passed, only generates 0/1 masks and skips the step of masking '
            'out the original input raster'))
    args = parser.parse_args()

    task_graph = taskgraph.TaskGraph(
        WORKSPACE_DIR, min(os.cpu_count(), len(args.percentile_to_path)))

    # sort so we get percentiles in the right order
    sorted_percentiles = sorted(
        args.percentile_to_path, key=lambda x: float(x.split('-')[1]))

    percentile_to_path_list = [
        arg.split('-') for arg in sorted_percentiles]

    percentile_value_task = task_graph.add_task(
        func=geoprocessing.raster_band_percentile,
        args=(
            (args.input_raster_path, 1), WORKSPACE_DIR,
            [float(x[1]) for x in percentile_to_path_list]),
        kwargs={
            'heap_buffer_size': 2**28,
            'ffi_buffer_size': 2**10
        },
        store_result=True,
        task_name='percentile find')

    percentile_value_list = percentile_value_task.get()
    LOGGER.debug(f'this is the percentile list ********** {percentile_value_list} from {percentile_to_path_list}')
    if args.mask_only:
        task_graph.join()
        task_graph.close()
        return

    for percentile_value, (op, _, target_raster_path) in zip(
            percentile_value_list, percentile_to_path_list):
        task_graph.add_task(
            func=mask_raster_by_value,
            args=(
                args.input_raster_path, percentile_value, op,
                target_raster_path),
            target_path_list=[target_raster_path],
            task_name=f'percentile for {target_raster_path}')
    LOGGER.info('all done!')

    task_graph.join()
    task_graph.close()


if __name__ == '__main__':
    main()
