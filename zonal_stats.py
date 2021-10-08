"""Command line script wrapping pygeoprocessing.raster_stats."""
import argparse
import datetime
import glob
import logging
import os
import pprint
import sys

from osgeo import gdal
import ecoshard.geoprocessing as pygeoprocessing
import taskgraph

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)

LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.INFO)

WORKSPACE_DIR = 'zonal_stats_workspace'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='mult by columns script')
    parser.add_argument(
        'raster_pattern', type=str,
        help='path or pattern to raster')
    parser.add_argument(
        'vector_path', type=str,
        help='path to raster')
    parser.add_argument(
        '--field_name', type=str,
        help='provide vector fieldname to summarize by, otherwise just fid')
    args = parser.parse_args()

    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, -1)

    LOGGER.info(
        f'calculating zonal stats for {args.raster_pattern} '
        f'on {args.vector_path}')
    working_dir = os.path.join(WORKSPACE_DIR, 'zonal_stats')
    os.makedirs(working_dir, exist_ok=True)
    for raster_path in glob.glob(args.raster_pattern):
        basename = os.path.basename(os.path.splitext(raster_path)[0])
        zonal_stats_task = task_graph.add_task(
            func=pygeoprocessing.zonal_statistics,
            args=((raster_path, 1), args.vector_path),
            kwargs={
                'working_dir': working_dir,
                'clean_working_dir': False,
                },
            store_result=True,
            task_name=(
                f'calculating zonal stats for {raster_path} '
                f'on {args.vector_path}'))

        stat_dict = zonal_stats_task.get()
        pp = pprint.PrettyPrinter(indent=4)
        fid_to_field_val = {}
        if args.field_name:
            vector = gdal.OpenEx(args.vector_path, gdal.OF_VECTOR)
            layer = vector.GetLayer()
            fid_to_field_val = {
                fid: layer.GetFeature(fid).GetField(args.field_name)
                for fid in stat_dict
            }
            layer = None
            vector = None
        time_str = str(datetime.datetime.utcnow()).replace(
            '-', '_').replace(':', '_').replace('.', '_').replace(' ', '_')
        stat_list = ['count', 'max', 'min', 'nodata_count', 'sum']
        table_path = os.path.join(WORKSPACE_DIR, f'{basename}_{time_str}.csv')
        LOGGER.info(f'building table at {table_path}')
        with open(table_path, 'w') as table_file:
            table_file.write(f'{raster_path}\n{args.vector_path}\n')
            table_file.write('fid,')
            if args.field_name:
                table_file.write(f'{args.field_name},')
            table_file.write(f'{",".join(stat_list)},mean\n')
            for fid, stats in stat_dict.items():
                table_file.write(f'{fid},')
                if args.field_name:
                    table_file.write(f'{fid_to_field_val[fid]},')
                for stat in stat_list:
                    table_file.write(f'{stats[stat]},')
                if stats['count'] > 0:
                    table_file.write(f'{stats["sum"]/stats["count"]}')
                else:
                    table_file.write(f'NaN')
                table_file.write('\n')
        LOGGER.info(f'all done, table at {table_path}')
