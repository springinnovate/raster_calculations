"""Average arbitrary set of rasters."""
from datetime import datetime
import argparse
import logging
import os
import sys

from ecoshard import geoprocessing
from ecoshard import taskgraph
import numpy


logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
logging.getLogger('ecoshard.taskgraph').setLevel(logging.WARNING)


def parse_ini(path):
    valid_keys = {'mask_raster', 'value_raster'}
    data = {}
    current_section = None
    with open(path, 'r') as f:
        lines = f.read().splitlines()

    for line in lines:
        line = line.strip()

        # Skip empty lines or comment lines
        if not line or line.startswith('#') or line.startswith(';'):
            continue

        # Check if it's a section header
        if line.startswith('[') and line.endswith(']'):
            section = line[1:-1].strip()
            if section in data:
                raise ValueError(f"Duplicate section '{section}' found.")
            data[section] = {}
            current_section = section
        else:
            # Parse key=value
            key, value = [part.strip() for part in line.split('=', 1)]
            if key not in valid_keys:
                raise ValueError(f"Invalid key '{key}' in section '{current_section}'.")
            data[current_section][key] = value

    return data


def align_and_resize_raster_stack(base_raster_path_list, aligned_raster_path_list):
    target_pixel_size = geoprocessing.get_raster_info(
        base_raster_path_list[0])['pixel_size']

    geoprocessing.align_and_resize_raster_stack(
        base_raster_path_list, aligned_raster_path_list, ['near'] * len(aligned_raster_path_list),
        target_pixel_size, 'union')


def mask_and_sum(mask_raster_path, value_raster_path):
    value_nodata = geoprocessing.get_raster_info(
        value_raster_path)['nodata'][0]
    path_list = [mask_raster_path, value_raster_path]

    masked_running_sum = 0
    for _, (mask_array, value_array) in geoprocessing.iterblocks(
            [(path, 1) for path in path_list],
            skip_sparse=True, allow_different_blocksize=True):
        valid_mask = (mask_array > 0) & ~numpy.isnan(value_array)
        if value_nodata is not None:
            valid_mask &= (value_array != value_nodata)
        masked_running_sum += numpy.sum(value_array[valid_mask])
    full_running_sum = 0
    for _, value_array in geoprocessing.iterblocks(
            [(path_list[1], 1)], skip_sparse=True, allow_different_blocksize=True):
        valid_mask = ~numpy.isnan(value_array)
        if value_nodata is not None:
            valid_mask &= (value_array != value_nodata)
        full_running_sum += numpy.sum(value_array[valid_mask])
    return masked_running_sum, full_running_sum


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Mask raster B by raster A and report non-nodata sum.')
    parser.add_argument(
        'diff_conf_path', help=""".ini file that has the format:

[idn_flood_inf]
mask_raster = path/to/idn_flood_mask_inf.tif
value_raster = path/to/idn_flood_value_inf.tif


[idn_flood_restoration]
mask_raster = path/to/idn_flood_mask_restoration.tif
value_raster = path/to/idn_flood_value_restoration.tif

...""")

    args = parser.parse_args()

    configuration = parse_ini(args.diff_conf_path)
    task_graph = taskgraph.TaskGraph('.', os.cpu_count(), 10.0)
    task_map = {}
    for key, file_lookup in configuration.items():
        local_workspace = os.path.join('mask_and_sum_workspace', key)
        os.makedirs(local_workspace, exist_ok=True)

        base_raster_path_list = [
            file_lookup['mask_raster'], file_lookup['value_raster']]
        aligned_raster_path_list = [
            os.path.join(local_workspace, os.path.basename(path))
            for path in base_raster_path_list]

        align_task = task_graph.add_task(
            func=align_and_resize_raster_stack,
            args=(base_raster_path_list, aligned_raster_path_list),
            target_path_list=aligned_raster_path_list,
            task_name=f'align for {key}')

        task = task_graph.add_task(
            func=mask_and_sum,
            args=aligned_raster_path_list,
            dependent_task_list=[align_task],
            store_result=True,
            task_name=f'sum {key}')
        task_map[key] = task

    table_path = f"{os.path.splitext(os.path.basename(args.diff_conf_path))[0]}_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.csv"
    with open(table_path, 'w') as file:
        file.write('sum_id,masked summed value,raw summed value\n')
        for key, task in task_map.items():
            masked_running_sum, full_running_sum = task.get()
            LOGGER.debug(f'writing {key},{masked_running_sum},{full_running_sum}')
            file.write(f'{key},{masked_running_sum},{full_running_sum}\n')

    LOGGER.info(f'results in {table_path}')
