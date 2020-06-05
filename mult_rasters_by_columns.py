"""Demo of how to use pandas to multiply one table by another."""
import argparse
import logging
import multiprocessing
import os
import sys

from osgeo import gdal
from osgeo import ogr
from osgeo import osr
import pandas
import pygeoprocessing
import taskgraph

gdal.SetCacheMax(2**30)

# treat this one column name as special for the y intercept
INTERCEPT_COLUMN_ID = 'intercept'
NCPUS = multiprocessing.cpu_count()

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)

LOGGER = logging.getLogger(__name__)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='mult by columns script')
    parser.add_argument(
        '--lasso_table_path', type=str, required=True,
        help='path to lasso table')
    parser.add_argument(
        '--data_dir', type=str, required=True,
        help='path to directory containing rasters in lasso table path')
    parser.add_argument(
        '--workspace_dir', type=str, required=True,
        help=(
            'path to output directory, will contain "result.tif" after '
            'completion'))

    args = parser.parse_args()

    LOGGER.info('TODO: align rasters here')

    LOGGER.info('parse lasso table path')
    lasso_table_path = args.lasso_table_path
    lasso_df = pandas.read_csv(lasso_table_path)

    target_df = pandas.DataFrame()

    header_pos = {}
    raster_symbol_list = []
    for row_index, row in lasso_df.iterrows():
        header = row[0]
        header_pos[header] = row_index

        lasso_val = row[1]

        product_list = header.split('*')
        exponent_list = []
        for product in product_list:
            if '^' in product:
                exponent_list.append(product.split('^'))
            else:
                exponent_list.append((product, 1))

        for symbol, exponent in exponent_list:
            if symbol not in raster_symbol_list:
                raster_symbol_list.append(symbol)

        LOGGER.debug(f'{lasso_val} * {exponent_list}')

    raster_symbol_to_path_map = {}
    missing_symbol_list = []
    for raster_symbol in raster_symbol_list:
        raster_path = os.path.join(args.data_dir, f'{raster_symbol}.tif')
        if not os.path.exists(raster_path):
            missing_symbol_list.append(raster_path)
    if missing_symbol_list:
        LOGGER.error(
            f'expected the following '
            f'{"rasters" if len(missing_symbol_list) > 1 else "raster"} given '
            f'the entries in the table, but could not find them locally:\n '
            "\n".join(missing_symbol_list))
        sys.exit(-1)


def raster_model(*raster_nodata_term_order_list):
    """Calculate summation of product power terms.

    Args:
        raster_nodata_term_order_list (list): a series of 4 elements for each
            raster in the order:
                * raster array slice
                * raster nodata
                * constant float term to multiply to this raster
                * integer (n) order term to exponentiate the raster (r) (r**n)
                * list of additional raster indexes to multiply this raster by

    Returns:
        each term evaluated, then the sum of all the terms.
    """
    pass
