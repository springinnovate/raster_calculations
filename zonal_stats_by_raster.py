"""Calculate stats per landcover code type."""
import argparse
import os
import logging
import hashlib
import multiprocessing
import time

from osgeo import gdal
from ecoshard import geoprocessing
from ecoshard import taskgraph
import numpy

gdal.SetCacheMax(2**26)
_LARGEST_BLOCK = 2**26

_GTIFF_CREATION_TUPLE_OPTIONS = ('GTIFF', (
    geoprocessing.DEFAULT_GTIFF_CREATION_TUPLE_OPTIONS[1]) +
    ('SPARSE_OK=TRUE',))

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(__name__)
logging.getLogger('ecoshard.taskgraph').setLevel(logging.INFO)


def mask_out_op(mask_data, base_data, mask_code, base_nodata):
    """Return 1 where base data == mask_code, 0 or nodata othewise."""
    result = numpy.full(base_data.shape, base_nodata, dtype=numpy.float32)
    valid_mask = (mask_data == mask_code) & (~numpy.isnan(base_data))
    if numpy.any(valid_mask):
        result[valid_mask] = base_data[valid_mask]
    else:
        result = None
    return result


def _calculate_stats(
        aligned_raster_path_list, mask_code, masked_nodata,
        masked_raster_path):
    LOGGER.debug(f'_calculate_stats for {masked_raster_path}')
    geoprocessing.raster_calculator(
        [(aligned_raster_path_list[0], 1), (aligned_raster_path_list[1], 1),
         (mask_code, 'raw'), (masked_nodata, 'raw')],
        mask_out_op, masked_raster_path, gdal.GDT_Float32,
        masked_nodata,
        raster_driver_creation_tuple=_GTIFF_CREATION_TUPLE_OPTIONS)
    masked_raster = gdal.OpenEx(masked_raster_path, gdal.OF_RASTER)
    masked_band = masked_raster.GetRasterBand(1)
    n_pixels = masked_band.XSize * masked_band.YSize
    LOGGER.debug(f'{n_pixels}')
    (raster_min, raster_max, raster_mean, raster_stdev) = (
        masked_band.GetStatistics(0, 1))
    masked_band = None
    masked_raster = None

    LOGGER.debug(f'getting stats for {masked_raster_path}')
    valid_count = 0
    for offset_dict, masked_block in geoprocessing.iterblocks(
            (masked_raster_path, 1), largest_block=_LARGEST_BLOCK,
            skip_sparse=True):
        valid_mask = (masked_block != masked_nodata)
        valid_count += numpy.count_nonzero(valid_mask)

    nodata_count = n_pixels - valid_count
    LOGGER.debug(f'{valid_count}')

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
    parser.add_argument(
        '--ndv', type=float, help=(
            'set the nodata value if one is not defined or you wish to '
            'override that value when calculating statistics'))
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

    task_graph = taskgraph.TaskGraph(args.working_dir, args.n_workers, 15.0)
    other_raster_info = geoprocessing.get_raster_info(args.other_raster)
    align_hash = hashlib.md5(
        f'{other_raster_info["pixel_size"]}_'
        f'{other_raster_info["projection_wkt"]}'.encode('utf-8')).hexdigest()
    aligned_raster_dir = os.path.join(
        args.working_dir, f'aligned_rasters_{align_hash}')
    os.makedirs(aligned_raster_dir, exist_ok=True)
    base_raster_path_list = [args.landcover_raster, args.other_raster]
    aligned_raster_path_list = [
        os.path.join(aligned_raster_dir, os.path.basename(path))
        for path in base_raster_path_list]
    other_nodata = other_raster_info['nodata'][0]
    if args.ndv is None and (
            other_nodata is None or
            not numpy.isfinite(other_raster_info['nodata'][0])):
        raise ValueError(
            f'nodata value undefined for {args.other_raster}, you must set it '
            f'using the --ndv flag. -9999 is a good value if you are unsure '
            f'of what to select.')
    if args.ndv is not None:
        other_nodata = args.ndv
    if not args.do_not_align and (args.landcover_raster != args.other_raster):

        # check if target pixel size is not much larger than base
        landcover_raster_info = geoprocessing.get_raster_info(
            args.landcover_raster)
        if any([abs(landcover_raster_info['pixel_size'][ix]) /
                abs(other_raster_info['pixel_size'][ix]) >= 2
                for ix in [0, 1]]):
            interpolation_list = ['mode', 'near']
        else:
            interpolation_list = ['near', 'near']

        align_task = task_graph.add_task(
            func=geoprocessing.align_and_resize_raster_stack,
            args=(
                base_raster_path_list, aligned_raster_path_list,
                interpolation_list, other_raster_info['pixel_size'],
                'intersection',
                ),
            kwargs={
                'target_projection_wkt': other_raster_info['projection_wkt'],
                'raster_driver_creation_tuple': _GTIFF_CREATION_TUPLE_OPTIONS,
                'gdal_warp_options': (
                    'warpMemoryLimit=1000', 'multithread=TRUE')},
            target_path_list=aligned_raster_path_list,
            task_name=f'aligning {aligned_raster_path_list}')
    else:
        aligned_raster_path_list = base_raster_path_list
        align_task = task_graph.add_task()
    task_graph.join()
    lulc_nodata = geoprocessing.get_raster_info(
        args.landcover_raster)['nodata']
    LOGGER.info('calculate unique values')
    unique_value_task = task_graph.add_task(
        func=geoprocessing.get_unique_values,
        args=((args.landcover_raster, 1),),
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
                aligned_raster_path_list, mask_code, other_nodata,
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
