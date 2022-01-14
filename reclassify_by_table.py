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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Reclassify raster by table')
    parser.add_argument('raster_path', type=str, help='path to raster')
    parser.add_argument(
        'reclassify_table_path', type=str, help='path to table')
    parser.add_argument(
        'base_lulc_field', type=str, help=(
            'column name in `reclassify_table_path` that corresponds to the '
            '"input" lulc in `raster_path`'))
    parser.add_argument(
        'target_value_field', type=str, help=(
            'column name in `reclassify_table_path` that corresponds to the '
            '"output" value in `raster_path`'))
    parser.add_argument(
        '--float', action='store_true',
        help='pass this flag if you want a floating point output')

    args = parser.parse_args()

    if args.float:
        def cast_fn(x):
            """Cast to float."""
            return float(x)
        target_type = gdal.GDT_Float32
    else:
        def cast_fn(x):
            """Cast to int."""
            return int(x)
        target_type = gdal.GDT_Int32

    df = pandas.read_csv(args.reclassify_table_path)
    value_map = {
        int(base_lucode): cast_fn(target_val)
        for base_lucode, target_val in zip(
            df[args.base_lulc_field], df[args.target_value_field])}
    LOGGER.info(f'reclassification map: {value_map}')

    target_raster_path = f'''reclassified_{
        _base_filename(args.raster_path)}_{
        _base_filename(args.reclassify_table_path)}.tif'''
    LOGGER.info(f'reclassifying to: {target_raster_path}')
    raster_info = geoprocessing.get_raster_info(args.raster_path)
    geoprocessing.reclassify_raster(
        (args.raster_path, 1), value_map, target_raster_path,
        target_type, raster_info['nodata'][0],
        values_required=False)
    LOGGER.info('all done!')
