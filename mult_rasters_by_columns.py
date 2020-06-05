"""Demo of how to use pandas to multiply one table by another."""
import argparse
import logging
import multiprocessing
import os
import sys

from osgeo import gdal
import pandas
import pygeoprocessing
import numpy
import taskgraph

gdal.SetCacheMax(2**30)

# treat this one column name as special for the y intercept
INTERCEPT_COLUMN_ID = 'intercept'
OPERATOR_FN = {
    '+': numpy.add,
    '*': numpy.multiply,
    '^': numpy.power,
}
N_CPUS = multiprocessing.cpu_count()

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)

LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.INFO)


def raster_rpn_calculator_op(*args_list):
    """Calculate RPN expression.

    Args:
        args_list (list): a length list of N+3 long where:
            - the first N elements are array followed by nodata
            - the N+1th element is the target nodata
            - the N+2nd  element is an RPN stack containing either
              symbols, numeric values, or an operator in OPERATOR_SET.
            - the last value is a dict mapping the symbol to a dict with
              "index" in it showing where index*2 location it is in the
              args_list.

    Returns:
        evaluation of the RPN calculation
    """
    n = len(args_list)-3
    result = numpy.empty(args_list[0].shape, dtype=numpy.float32)
    result[:] = args_list[n]  # target nodata
    valid_mask = numpy.ones(args_list[0].shape, dtype=numpy.bool)
    # build up valid mask where all pixel stacks are defined
    for index in range(0, n, 2):
        LOGGER.debug(f'{index}: check if {args_list[index]} is close to {args_list[index+1]}')
        nodata_value = args_list[index+1]
        if nodata_value is not None:
            valid_mask &= ~numpy.isclose(args_list[index], args_list[index+1])
    rpn_stack = list(args_list[-2])
    info_dict = args_list[-1]
    LOGGER.debug(info_dict)

    # process the rpn stack
    accumulator_stack = []
    while rpn_stack:
        val = rpn_stack.pop(0)
        if val in OPERATOR_FN:
            operator = val
            operand_b = accumulator_stack.pop()
            operand_a = accumulator_stack.pop()
            LOGGER.debug(
                f"{operator}({operand_a},{operand_b})")
            val = OPERATOR_FN[operator](operand_a, operand_b)
            accumulator_stack.append(val)
        else:
            if isinstance(val, str):
                accumulator_stack.append(
                    args_list[2*info_dict[val]['index']][valid_mask])
            else:
                accumulator_stack.append(val)

    result[valid_mask] = accumulator_stack.pop(0)
    if accumulator_stack:
        raise RuntimeError(
            f'accumulator_stack not empty: {accumulator_stack}')
    return result


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
    parser.add_argument(
        '--target_nodata', type=float, default=numpy.finfo('float32').min,
        help='desired target nodata value')

    args = parser.parse_args()

    LOGGER.info('parse lasso table path and build expression')
    lasso_table_path = args.lasso_table_path
    lasso_df = pandas.read_csv(lasso_table_path, header=None)

    # built a reverse polish notation stack for the operations and their order
    # that they need to be executed in
    rpn_stack = []
    first_term = True
    for row_index, row in lasso_df.iterrows():
        header = row[0]
        if header == INTERCEPT_COLUMN_ID:
            # special case of the intercept, just push it
            rpn_stack.append(float(row[1]))
        else:
            # it's an expression/coefficient row
            LOGGER.debug(f'{row_index}: {row}')
            coefficient = float(row[1])
            # put on the coefficient first since it's there, we'll multiply
            # it later
            rpn_stack.append(coefficient)

            # split out all the multiplcation terms
            product_list = header.split('*')
            for product in product_list:
                # for each multiplcation term split out an exponent if exists
                if '^' in product:
                    rpn_stack.extend(product.split('^'))
                    # cast the exponent to an integer so can operate directly
                    rpn_stack[-1] = int(rpn_stack[-1])
                    # push the ^ to exponentiate the last two operations
                    rpn_stack.append('^')
                else:
                    # otherwise it's a single value
                    rpn_stack.append(product)
                # multiply this term and the last
                rpn_stack.append('*')

        # if it's not the first term we want to add the rest
        if first_term:
            first_term = False
        else:
            rpn_stack.append('+')

    LOGGER.debug(rpn_stack)

    # find the unique symbols in the expression
    raster_symbol_list = [
        x for x in set(rpn_stack)-set(OPERATOR_FN)
        if not isinstance(x, (int, float))]

    LOGGER.debug(raster_symbol_list)

    # translate symbols into raster paths and get relevant raster info
    raster_symbol_to_info_map = {}
    missing_symbol_list = []
    min_size = sys.float_info.max
    bounding_box_list = []
    for index, raster_symbol in enumerate(raster_symbol_list):
        raster_path = os.path.join(args.data_dir, f'{raster_symbol}.tif')
        if not os.path.exists(raster_path):
            missing_symbol_list.append(raster_path)
            continue
        else:
            raster_info = pygeoprocessing.get_raster_info(raster_path)
            raster_symbol_to_info_map[raster_symbol] = {
                'path': raster_path,
                'nodata': raster_info['nodata'][0],
                'index': index,
            }
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
        f'raster paths:\n{str(raster_symbol_to_info_map)}')

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
    for raster_id in raster_symbol_to_info_map:
        raster_path = raster_symbol_to_info_map[raster_id]['path']
        raster_basename = os.path.splitext(os.path.basename(raster_path))[0]
        aligned_raster_path = os.path.join(
            align_dir,
            f'{raster_basename}_{target_bounding_box}_{target_pixel_size}.tif')
        raster_symbol_to_info_map[raster_id]['aligned_path'] = \
            aligned_raster_path
        task_graph.add_task(
            func=pygeoprocessing.warp_raster,
            args=(
                raster_path, target_pixel_size, aligned_raster_path,
                'near'),
            kwargs={
                'target_bb': target_bounding_box,
                'working_dir': args.workspace_dir
            })

    LOGGER.info('construct raster calculator raster path band list')
    raster_path_band_list = []
    for raster_symbol in raster_symbol_list:
        raster_path_band_list.append(
            (raster_symbol_to_info_map[raster_id]['aligned_path'], 1))
        raster_path_band_list.append(
            (raster_symbol_to_info_map[raster_id]['nodata'], 'raw'))
    raster_path_band_list.append((args.target_nodata, 'raw'))
    raster_path_band_list.append((rpn_stack, 'raw'))
    raster_path_band_list.append((raster_symbol_to_info_map, 'raw'))

    LOGGER.debug(rpn_stack)

    # wait for rasters to align
    task_graph.join()
    task_graph.close()
    task_graph._terminate()

    result_path = os.path.join(args.workspace_dir, 'result.tif')
    pygeoprocessing.raster_calculator(
        raster_path_band_list, raster_rpn_calculator_op, result_path,
        gdal.GDT_Float32, float(args.target_nodata))
    LOGGER.debug('all done')
