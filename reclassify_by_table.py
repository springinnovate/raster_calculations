"""Reclassify raster by given table."""
import argparse
import logging
import os

from ecoshard import geoprocessing
from osgeo import gdal
import pandas

gdal.SetCacheMax(2**28)
logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(__name__)


def _base_filename(path):
    """Get the filename without dir or extension."""
    return os.path.basename(os.path.splitext(path)[0])


INT_TYPE = 'int'
FLOAT_TYPE = 'float'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Reclassify raster by table')
    parser.add_argument('raster_path', type=str, help='path to raster')
    parser.add_argument(
        'reclassify_table_path', type=str, help='path to table')
    parser.add_argument(
        'base_field', type=str, help=(
            'column name in `reclassify_table_path` that corresponds to the '
            'values to map from in `raster_path`'))
    parser.add_argument(
        'target_value_fields', nargs='+', type=str, help=(
            'column name,output_type where `column_name` is a column in '
            f'`reclassify_table_path` and `output_type` is either '
            f'`{INT_TYPE}` or `{FLOAT_TYPE}` representing the target output '
            'type.'))

    args = parser.parse_args()

    table = pandas.read_csv(args.reclassify_table_path)
    if args.base_field not in table:
        raise ValueError(
            f'expected a field called `{args.base_field}` in '
            f'{args.reclassify_table_path} but only found these columns: '
            f'{table.columns}')

    for column_name_type_pair in args.target_value_fields:
        try:
            column_name, target_type = column_name_type_pair.split(',')
        except ValueError:
            LOGGER.error(
                f'expected a comma separated `column name,output_type` pair '
                f'but instead got `{column_name_type_pair}`')
            raise
        if column_name not in table:
            raise ValueError(
                f'field `{column_name}` is not a column in '
                f'`{args.reclassify_table_path}`, these are the columns present '
                f'{table.columns}')

        if target_type not in [FLOAT_TYPE, INT_TYPE]:
            raise ValueError(
                f'expected either `float` or `int` for type pair but instead '
                f'got `{target_type}` from this argument pair: '
                f'`{column_name_type_pair}`')

        if target_type == INT_TYPE and any(
                not isinstance(x, int) and not x.is_integer() for x in table[
                    column_name]):
            LOGGER.warn(
                f'There are floating point values in {column_name} '
                f'column but the type pair was `{INT_TYPE}, just make sure '
                'that\'s what you want to do.')

        if target_type == FLOAT_TYPE:
            def cast_fn(x):
                """Cast to float."""
                return float(x)
            raster_target_type = gdal.GDT_Float32
        else:
            def cast_fn(x):
                """Cast to int."""
                return int(x)
            raster_target_type = gdal.GDT_Int32

        value_map = {
            int(base_lucode): cast_fn(target_val)
            for base_lucode, target_val in zip(
                table[args.base_field], table[column_name])}
        LOGGER.info(f'reclassification map: {value_map}')

        target_raster_path = f'''reclassified_{
            _base_filename(args.raster_path)}_{
            _base_filename(args.reclassify_table_path)}_{
            column_name}.tif'''
        LOGGER.info(f'reclassifying to: {target_raster_path}')
        raster_info = geoprocessing.get_raster_info(args.raster_path)
        geoprocessing.reclassify_raster(
            (args.raster_path, 1), value_map, target_raster_path,
            raster_target_type, raster_info['nodata'][0])
        LOGGER.info(
            f'reclassified {column_name_type_pair} to {target_raster_path}')
