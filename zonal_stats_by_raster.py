"""Calculate stats per landcover code type."""
import argparse
import os
import logging
import hashlib
import sys
import multiprocessing
import time

from osgeo import gdal
from ecoshard import geoprocessing
from ecoshard import taskgraph
import numpy

gdal.SetCacheMax(2**26)


logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
logging.getLogger('ecoshard.taskgraph').setLevel(logging.INFO)


def get_unique_values(raster_path):
    """Return a list of non-nodata unique values from `raster_path`."""
    nodata = geoprocessing.get_raster_info(raster_path)['nodata'][0]
    unique_set = set()
    block_list_len = len(list(geoprocessing.iterblocks(
        (raster_path, 1), offset_only=True, largest_block=2**24)))
    last_time = time.time()
    for block_id, (offset_data, array) in enumerate(geoprocessing.iterblocks(
            (raster_path, 1), largest_block=2**30)):
        if time.time()-last_time > 5.0:
            LOGGER.info(
                f'{(block_id+1)/(block_list_len)*100:.2f}% complete on '
                f'{raster_path}. set size: {len(unique_set)}')
            last_time = time.time()
        if nodata is not None:
            valid_mask = array != nodata
            unique_set |= set(numpy.unique(array[valid_mask]))
        else:
            unique_set |= set(numpy.unique(array))
    return unique_set


def mask_out_op(mask_data, base_data, mask_code, base_nodata):
    """Return 1 where base data == mask_code, 0 or nodata othewise."""
    result = numpy.empty_like(base_data)
    result[:] = base_nodata
    valid_mask = (mask_data == mask_code) & (~numpy.isnan(base_data))
    result[valid_mask] = base_data[valid_mask]
    return result


def _calculate_stats(
        aligned_raster_path_list, mask_code, other_raster_info,
        masked_raster_path):
    LOGGER.debug(f'_calculate_stats for {masked_raster_path}')
    geoprocessing.raster_calculator(
        [(aligned_raster_path_list[0], 1), (aligned_raster_path_list[1], 1),
         (mask_code, 'raw'), (other_raster_info['nodata'][0], 'raw')],
        mask_out_op, masked_raster_path, gdal.GDT_Float32,
        other_raster_info['nodata'][0])
    masked_raster = gdal.OpenEx(masked_raster_path, gdal.OF_RASTER)
    masked_band = masked_raster.GetRasterBand(1)
    (raster_min, raster_max, raster_mean, raster_stdev) = (
        masked_band.GetStatistics(0, 1))
    value_nodata = other_raster_info['nodata'][0]
    masked_band = None
    masked_raster = None

    LOGGER.debug(f'getting stats for {masked_raster_path}')
    nodata_count = 0
    valid_count = 0
    value_raster = gdal.OpenEx(aligned_raster_path_list[1], gdal.OF_RASTER)
    value_band = value_raster.GetRasterBand(1)
    for offset_dict, base_block in geoprocessing.iterblocks(
            (aligned_raster_path_list[0], 1), largest_block=2**30):
        valid_mask = base_block == mask_code
        value_block = value_band.ReadAsArray(**offset_dict)
        valid_value_block = value_block[valid_mask]
        if value_nodata is not None:
            local_nodata_count = numpy.count_nonzero(
                numpy.isclose(valid_value_block, value_nodata))
        else:
            local_nodata_count = 0
        nodata_count += local_nodata_count
        valid_count += valid_value_block.size - local_nodata_count

    value_raster = None
    value_band = None

    return (raster_min, raster_max, raster_mean,
            raster_stdev, valid_count, nodata_count)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Zonal stats by raster')
    parser.add_argument(
        'landcover_raster', help='Path to landcover raster.')
    parser.add_argument(
        'other_raster', help='Path to another raster to calculate stats over.')
    parser.add_argument(
        '--working_dir', default='lulc_raster_stats_workspace',
        help='location to store temporary files')
    parser.add_argument(
        '--do_not_align', default=False, action='store_true',
        help='pass this flag to avoid aligning rasters')
    parser.add_argument('--basename', type=str, help=(
        'output table will include this name, if left off a unique hash will '
        'be created from the landcover and other raster filepath and '
        'timestamp strings, this means a new subworkspace will be created on '
        'each run.'))
    parser.add_argument(
        '--n_workers', type=int, default=multiprocessing.cpu_count(),
        help=(
            'number of CPUs to use for processing, default is all CPUs on '
            'the machine'))
    args = parser.parse_args()
    if args.basename:
        basename = args.basename
    else:
        basename = hashlib.sha1(
            f'{args.landcover_raster}_{args.other_raster}'.encode(
                'utf-8')).hexdigest()[:12]
    basename += '_'+time.strftime("%Y_%m_%d_%H_%M_%S", time.gmtime())
    working_dir = os.path.join(args.working_dir, basename)
    os.makedirs(working_dir, exist_ok=True)

    task_graph = taskgraph.TaskGraph(
        working_dir, args.n_workers, 10.0)

    base_raster_path_list = [args.landcover_raster, args.other_raster]
    aligned_raster_path_list = [
        os.path.join(working_dir, os.path.basename(path))
        for path in base_raster_path_list]
    other_raster_info = geoprocessing.get_raster_info(
        args.other_raster)
    if not args.do_not_align and (args.landcover_raster != args.other_raster):
        align_task = task_graph.add_task(
            func=geoprocessing.align_and_resize_raster_stack,
            args=(
                base_raster_path_list, aligned_raster_path_list,
                ['mode', 'near'], other_raster_info['pixel_size'],
                'intersection',
                ),
            kwargs={
                'target_projection_wkt': other_raster_info['projection_wkt']},
            target_path_list=aligned_raster_path_list,
            task_name=f'aligning {aligned_raster_path_list}')
    else:
        aligned_raster_path_list = base_raster_path_list
    task_graph.join()
    lulc_nodata = geoprocessing.get_raster_info(
        args.landcover_raster)['nodata']
    LOGGER.info('calculate unique values')
    unique_value_task = task_graph.add_task(
        func=get_unique_values,
        args=(args.landcover_raster,),
        store_result=True,
        dependent_task_list=[align_task],
        task_name=f'unique values for {args.landcover_raster}')
    unique_values = unique_value_task.get()
    LOGGER.debug(unique_values)
    stats_table = open(f'stats_table_{basename}.csv', 'w')
    stats_table.write(
        'lucode,min,max,mean,stdev,valid_count,nodata_count,total\n')

    masked_stats_list = []
    for mask_code in sorted(unique_values):
        LOGGER.debug(f'scheduling {mask_code}')
        masked_raster_path = os.path.join(working_dir, '%d.tif' % mask_code)
        stats_task = task_graph.add_task(
            func=_calculate_stats,
            args=(
                aligned_raster_path_list, mask_code, other_raster_info,
                masked_raster_path),
            store_result=True,
            dependent_task_list=[unique_value_task],
            target_path_list=[masked_raster_path],
            task_name=f'mask {masked_raster_path}')
        masked_stats_list.append((stats_task, mask_code))

    LOGGER.debug('waiting for it to gadot')
    for masked_task, mask_code in masked_stats_list:
        (raster_min, raster_max, raster_mean,
         raster_stdev, valid_count, nodata_count) = masked_task.get()
        stats_table.write(
            '%d,%f,%f,%f,%f,%d,%d,%d\n' % (
                mask_code, raster_min, raster_max, raster_mean, raster_stdev,
                valid_count, nodata_count, valid_count+nodata_count))
    stats_table.close()

    task_graph.join()
    task_graph.close()
