"""Align one raster to another."""
import argparse
import logging
import os
import sys

from ecoshard import geoprocessing
import numpy

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=(
        'Align raster A to raster B as a new raster such that '
        'raster A will be the same bounding box as B.'))
    parser.add_argument('raster_a_path', type=str, help='path to raster A (input)')
    parser.add_argument('raster_b_path', type=str, help='path to raster B (context)')
    parser.add_argument(
        'target_raster_path', type=str, help='path to name of aligned raster A')
    parser.add_argument(
        '--overwrite', action='store_true',
        help='Pass this flag to overwrite flags.')
    args = parser.parse_args()
    if os.path.exists(args.target_raster_path) and not args.overwrite:
        LOGGER.error(
            f'the target {args.target_raster_path} exists. to overwrite this, add '
            'the --overwrite flag and run again')
        sys.exit(-1)

    raster_a_info = geoprocessing.get_raster_info(args.raster_a_path)
    a_nodata = raster_a_info['nodata'][0]
    raster_b_info = geoprocessing.get_raster_info(args.raster_b_path)
    b_nodata = raster_b_info['nodata'][0]

    LOGGER.info(
        f'warping {args.raster_a_path} to align with {args.raster_b_path} '
        f'to {args.target_raster_path}')
    geoprocessing.warp_raster(
        args.raster_a_path, raster_b_info['pixel_size'],
        args.target_raster_path,
        'near', target_bb=raster_b_info['bounding_box'],
        target_projection_wkt=raster_b_info['projection_wkt'],
        working_dir=os.path.dirname(args.target_raster_path))
