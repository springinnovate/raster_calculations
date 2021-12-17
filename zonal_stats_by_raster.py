"""Calculate stats per landcover code type."""
import argparse
import os
import logging
import shutil
import sys
import tempfile
import multiprocessing

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
    for offset_data, array in geoprocessing.iterblocks((raster_path, 1)):
        unique_set |= set(numpy.unique(array[~numpy.isclose(array, nodata)]))
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
        mask_raster_path):
    LOGGER.debug(f'_calculate_stats for {mask_raster_path}')
    geoprocessing.raster_calculator(
        [(aligned_raster_path_list[0], 1), (aligned_raster_path_list[1], 1),
         (mask_code, 'raw'), (other_raster_info['nodata'][0], 'raw')],
        mask_out_op, mask_raster_path, gdal.GDT_Float32,
        other_raster_info['nodata'][0])
    raster = gdal.OpenEx(mask_raster_path, gdal.OF_RASTER)
    band = raster.GetRasterBand(1)
    _ = (band.GetStatistics(0, 1))
    band = None
    raster = None


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
        '--do_not_align', default=False, action='store_true', help='pass this flag to avoid aligning rasters')
    args = parser.parse_args()

    basename = f'''{
        os.path.splitext(os.path.basename(args.landcover_raster))[0][:40]}_''' + \
        f'{os.path.splitext(os.path.basename(args.other_raster))[0][:12]}'

    task_graph = taskgraph.TaskGraph(
        args.working_dir, multiprocessing.cpu_count(), 10.0)

    base_raster_path_list = [args.landcover_raster, args.other_raster]
    aligned_raster_path_list = [
        os.path.join(args.working_dir, os.path.basename(path))
        for path in base_raster_path_list]
    other_raster_info = geoprocessing.get_raster_info(
        args.other_raster)
    if not args.do_not_align:
        task_graph.add_task(
            func=geoprocessing.align_and_resize_raster_stack,
            args=(
                base_raster_path_list, aligned_raster_path_list, ['mode', 'near'],
                other_raster_info['pixel_size'], 'intersection',
                ),
            kwargs={'target_projection_wkt': other_raster_info['projection_wkt']},
            target_path_list=aligned_raster_path_list,
            task_name=f'aligning {aligned_raster_path_list}')
    else:
        aligned_raster_path_list = base_raster_path_list
    task_graph.join()
    lulc_nodata = geoprocessing.get_raster_info(
        args.landcover_raster)['nodata']
    unique_values = get_unique_values(args.landcover_raster)
    LOGGER.debug(unique_values)
    stats_table = open(f'stats_table_{basename}.csv', 'w')
    stats_table.write('lucode,min,max,mean,stdev,valid_count,nodata_count,total\n')

    mask_raster_path_list = []
    for mask_code in sorted(unique_values):
        LOGGER.debug(f'scheduling {mask_code}')
        mask_raster_path = os.path.join(args.working_dir, '%d.tif' % mask_code)
        mask_raster_path_list.append((mask_raster_path, mask_code))
        task_graph.add_task(
            func=_calculate_stats,
            args=(
                aligned_raster_path_list, mask_code, other_raster_info,
                mask_raster_path),
            target_path_list=[mask_raster_path],
            task_name=f'mask {mask_raster_path}')
    task_graph.close()
    task_graph.join()
    LOGGER.debug('waiting for it to gadot')
    for mask_raster_path, mask_code in mask_raster_path_list:
        LOGGER.debug(f'getting stats for {mask_raster_path}')
        raster = gdal.OpenEx(mask_raster_path, gdal.OF_RASTER)
        band = raster.GetRasterBand(1)
        (raster_min, raster_max, raster_mean, raster_stdev) = (
            band.GetStatistics(0, 1))
        n_pixels = band.XSize * band.YSize
        nodata_count = 0
        nodata = band.GetNoDataValue()
        if nodata is not None:
            for _, block in geoprocessing.iterblocks((mask_raster_path, 1)):
                nodata_mask = block == nodata
                nodata_count += numpy.count_nonzero(nodata_mask)
        band = None
        raster = None
        stats_table.write(
            '%d,%f,%f,%f,%f,%d,%d,%d\n' % (
                mask_code, raster_min, raster_max, raster_mean, raster_stdev,
                n_pixels-nodata_count, nodata_count, n_pixels))
    stats_table.close()
