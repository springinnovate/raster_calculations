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
    # Justin said this was his reference

    lasso_table_path = args.lasso_table_path

    print('reading')
    lasso_df = pandas.read_csv(lasso_table_path)

    target_df = pandas.DataFrame()

    header_pos = {}
    for row_index, row in lasso_df.iterrows():
        header = row[0]
        header_pos[header] = row_index

        lasso_val = row[1]

        LOGGER.debug(f'{header}*{lasso_val}')
        if '*' in header:
            # add a new column
            product_list = header.split('*')
        if '^' in header:
            raster_id, exponent = header.split('^')


def raster_model(*raster_nodata_term_order_list):
    """Calculate summation of terms accounting for

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
