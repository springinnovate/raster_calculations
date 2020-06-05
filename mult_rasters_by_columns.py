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
N_CPUS = multiprocessing.cpu_count()

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)

LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.INFO)


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
    parser.add_argument(
        '--bounding_box', type=float, nargs=4,
        help=(
            "manual bounding box in the form of four consecutive floats: "
            "min_lng, min_lat, max_lng, max_lat, ex: "
            "-180.0, -58.3, 180.0, 81.5"))
    parser.add_argument(
        '--pixel_size', type=float,
        help="desired target pixel size in raster units")
    parser.add_argument(
        '--zero_nodata', action='store_true',
        help=(
            'if present, treat nodata values as 0, if absent any nodata '
            'pixel in a stack will cause the output pixel to be nodata'))

    args = parser.parse_args()

    LOGGER.info('TODO: align rasters here')

    LOGGER.info('parse lasso table path')
    lasso_table_path = args.lasso_table_path
    lasso_df = pandas.read_csv(lasso_table_path, header=None)

    raster_symbol_list = []
    exponent_list = []
    constant_list = []
    for row_index, row in lasso_df.iterrows():
        header = row[0]
        LOGGER.debug(f'{row_index}: {row}')

        lasso_val = row[1]
        constant_list.append(lasso_val)

        product_list = header.split('*')
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
    min_size = sys.float_info.max
    bounding_box_list = []
    for raster_symbol in set(raster_symbol_list)-set([INTERCEPT_COLUMN_ID]):
        raster_path = os.path.join(args.data_dir, f'{raster_symbol}.tif')
        if not os.path.exists(raster_path):
            missing_symbol_list.append(raster_path)
            continue
        else:
            raster_info = pygeoprocessing.get_raster_info(raster_path)
            raster_symbol_to_path_map[raster_symbol] = raster_path
            min_size = min(
                min_size, abs(raster_info['pixel_size'][0]))
            bounding_box_list.append(raster_info['bounding_box'])

    if missing_symbol_list:
        LOGGER.error(
            f'expected the following '
            f'{"rasters" if len(missing_symbol_list) > 1 else "raster"} given '
            f'the entries in the table, but could not find them locally:\n'
            + "\n".join(missing_symbol_list))
        sys.exit(-1)

    LOGGER.info(
        f'raster paths:\n{str(raster_symbol_to_path_map)}')

    if args.bounding_box:
        target_bounding_box = args.bounding_box
    else:
        target_bounding_box = pygeoprocessing.merge_bounding_box_list(
            bounding_box_list, 'intersection')

    if args.pixel_size:
        target_pixel_size = (args.pixel_size, -args.pixel_size)
    else:
        target_pixel_size = (min_size, -min_size)

    LOGGER.info(f'target pixel size: {target_pixel_size}')
    LOGGER.info(f'target bounding box: {target_bounding_box}')

    LOGGER.debug('align rasters, this might take a while')
    task_graph = taskgraph.TaskGraph(args.workspace_dir, N_CPUS, 5.0)
    align_dir = os.path.join(args.workspace_dir, 'aligned_rasters')
    try:
        os.makedirs(align_dir)
    except OSError:
        pass

    # align rasters and cast to list because we'll rewrite
    # raster_symbol_to_path_map object
    for raster_id, raster_path in list(raster_symbol_to_path_map.items()):
        raster_basename = os.path.splitext(os.path.basename(raster_path))[0]
        aligned_raster_path = os.path.join(
            align_dir,
            f'{raster_basename}_{target_bounding_box}_{target_pixel_size}.tif')
        task_graph.add_task(
            func=pygeoprocessing.warp_raster,
            args=(
                raster_path, target_pixel_size, aligned_raster_path,
                'near'),
            kwargs={
                'target_bb': target_bounding_box,
                'working_dir': args.workspace_dir
            })

    LOGGER.info('construct raster calculator list')
    LOGGER.debug(f'{exponent_list}, {constant_list}')

    # wait for rasters to align
    task_graph.join()
    task_graph.close()
    LOGGER.debug('all done')


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
