"""Command line script wrapping geoprocessing.raster_stats."""
import argparse
import collections
import datetime
import glob
import logging
import os
import sys

from osgeo import gdal
from ecoshard import geoprocessing

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
    parser.add_argument(
        '--keep_working_dir', action='store_true',
        help='keep the temporary working directory created by zonal_stats')
    parser.add_argument(
        '--polygons_overlap', action='store_true', help=(
            'set if polygons overlap and need zonal_statistics to be '
            'calculated individually for each'))
    args = parser.parse_args()

    LOGGER.info(
        f'calculating zonal stats for {args.raster_pattern} '
        f'on {args.vector_path}')
    working_dir = os.path.join(WORKSPACE_DIR, 'zonal_stats')
    os.makedirs(working_dir, exist_ok=True)
    for raster_path in glob.glob(args.raster_pattern):
        LOGGER.info(f'processing {raster_path}')
        basename = os.path.basename(os.path.splitext(raster_path)[0])
        stat_dict = geoprocessing.zonal_statistics(
            (raster_path, 1), args.vector_path,
            working_dir=working_dir,
            clean_working_dir=not args.keep_working_dir,
            polygons_might_overlap=args.polygons_overlap)
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
        if args.field_name:
            stat_by_fieldname_dict = collections.defaultdict(dict)
            for fid, stats in stat_dict.items():
                field_val = fid_to_field_val[fid]
                field_stat_dict = stat_by_fieldname_dict[field_val]
                for stat_id in stat_list:
                    if stat_id not in field_stat_dict:
                        field_stat_dict[stat_id] = stats[stat_id]
                    else:
                        if stat_id == 'max':
                            if stats['max'] is None:
                                continue
                            if 'max' not in field_stat_dict:
                                field_stat_dict['max'] = stats[stat_id]
                            else:
                                field_stat_dict[stat_id] = max(
                                    field_stat_dict[stat_id],
                                    stats[stat_id])
                        elif stat_id == 'min':
                            if stats['min'] is None:
                                continue
                            if 'min' not in field_stat_dict:
                                field_stat_dict['min'] = stats[stat_id]
                            else:
                                field_stat_dict[stat_id] = min(
                                    field_stat_dict[stat_id],
                                    stats[stat_id])
                        else:
                            field_stat_dict[stat_id] += stats[stat_id]
                stat_by_fieldname_dict[field_val] = field_stat_dict

        table_path = os.path.join(WORKSPACE_DIR, f'{basename}_{time_str}.csv')
        LOGGER.info(f'*********** building table at {table_path}')
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
                for stat_id in stat_list:
                    table_file.write(f'{stats[stat_id]},')
                if stats['count'] > 0:
                    table_file.write(f'{stats["sum"]/stats["count"]}')
                else:
                    table_file.write('NaN')
                table_file.write('\n')
            if args.field_name:
                for field_name, stats in stat_by_fieldname_dict.items():
                    table_file.write(f'BY FIELD,{field_name},')
                    for stat_id in stat_list:
                        table_file.write(f'{stats[stat_id]},')
                    if stats['count'] > 0:
                        table_file.write(f'{stats["sum"]/stats["count"]}')
                    else:
                        table_file.write('NaN')
                    table_file.write('\n')

        LOGGER.info(f'all done, table at {table_path}')
