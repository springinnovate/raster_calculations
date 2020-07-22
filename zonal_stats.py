import argparse
import logging
import pprint
import sys

from osgeo import gdal
import pygeoprocessing
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
        'raster_path', type=str,
        help='path to raster')
    parser.add_argument(
        'vector_path', type=str,
        help='path to raster')
    parser.add_argument(
        '--field_name', type=str,
        help='provide vector fieldname to summarize by, otherwise just fid')
    args = parser.parse_args()

    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, -1)

    zonal_stats_task = task_graph.add_task(
        func=pygeoprocessing.zonal_statistics,
        args=((args.raster_path, 1), args.vector_path),
        task_name=(
            f'calculating zonal stats for {args.raster_path} '
            f'on {args.vector_path}'))

    stat_dict = zonal_stats_task.get()
    pp = pprint.PrettyPrinter(indent=4)
    if args.field_name:
        vector = gdal.OpenEx(args.vector_path, gdal.OF_VECTOR)
        layer = vector.GetLayer()
        stat_dict = {
            layer.GetFeature(fid).GetField(args.field_name): value
            for fid, value in stat_dict.items()
        }
    LOGGER.info(
        f'\nstats for {args.raster_path} on \n'
        f'{args.vector_path}:\n{pp.pformat(stat_dict)}')
