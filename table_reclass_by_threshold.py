"""Table based reclassify triggered by probability threshold."""
import argparse
import os
import logging
import hashlib

from ecoshard import geoprocessing
from ecoshard import taskgraph
import pandas

from osgeo import gdal

gdal.SetCacheMax(2**27)
logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
logging.getLogger('ecoshard.taskgraph').setLevel(logging.WARN)
LOGGER = logging.getLogger(__name__)

ALIGNED_DIR = 'reclass_aligned_dir'
os.makedirs(ALIGNED_DIR, exist_ok=True)


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(
        description='reclassify raster to table based on probability threshold')
    parser.add_argument(
        '--base_raster_path', type=str, required=True,
        help='path to integer raster')
    parser.add_argument(
        '--threshold_raster_path', type=str, required=True,
        help='path to threshold raster')
    parser.add_argument(
        '--threshold_value', type=float, required=True, help=(
            'floating point value, if threshold raster is greater than this '
            'value, reclassify based on > column of table.value, else use the '
            '<= value flip lulc pixel'))
    parser.add_argument(
        '--reclassify_table_path', type=str, required=True, help=(
            'path to csv table with columns'))
    parser.add_argument(
        '--csv_table_fields', type=str, nargs=3, required=True, help=(
            'column names for (1) base raster value, (2) value to flip to if <= '
            'threshold, and (3) value to flip to if > threshold'))
    parser.add_argument(
        '--target_raster_path', type=str,
        help='desired target raster')
    args = parser.parse_args()
    print(args.csv_table_fields)
    df = pandas.read_csv(args.reclassify_table_path)
    value_map = {
        int(base_lucode): (float(leq_target), float(gt_target))
        for (base_lucode, leq_target, gt_target) in zip(
            df[args.csv_table_fields[0]],
            df[args.csv_table_fields[1]],
            df[args.csv_table_fields[2]])
        }
    print(value_map)

    def _reclass_op(base_array, threshold_array):
        result = base_array.copy()
        for base_code, (leq_target, gt_target) in value_map.items():
            leq_mask = (
                base_array == base_code) * (
                    threshold_array <= args.threshold_value)
            result[leq_mask] = leq_target

            gt_mask = (
                base_array == base_code) * (
                    threshold_array > args.threshold_value)
            result[gt_mask] = gt_target
        return result

    base_raster_info = geoprocessing.get_raster_info(args.base_raster_path)

    base_raster_path_list = [
        args.base_raster_path, args.threshold_raster_path]
    path_hash = hashlib.sha256()
    path_hash.update(','.join([
        os.path.basename(path) for path in base_raster_path_list]))
    workspace_dir = os.path.join(ALIGNED_DIR, path_hash.hexdigest())
    os.makedirs(workspace_dir, exist_ok=True)
    aligned_raster_path_list = [
        os.path.join(workspace_dir, os.path.basename(path))
        for path in base_raster_path_list]
    LOGGER.info(f'aligning {base_raster_path_list}')
    task_graph = taskgraph.TaskGraph(workspace_dir, -1)
    task_graph.add_task(
        func=geoprocessing.align_and_resize_raster_stack,
        args=(
            base_raster_path_list, aligned_raster_path_list, ['near']*2,
            base_raster_info['pixel_size'], 'intersection'),
        kwargs={
            'target_projection_wkt': base_raster_info['projection_wkt']
        })
    task_graph.close()
    task_graph.join()

    LOGGER.info(f'reclassifying to {args.target_raster_path}')
    geoprocessing.raster_calculator(
        [(path, 1) for path in aligned_raster_path_list],
        _reclass_op, args.target_raster_path, base_raster_info['datatype'],
        base_raster_info['nodata'][0])


if __name__ == '__main__':
    main()
