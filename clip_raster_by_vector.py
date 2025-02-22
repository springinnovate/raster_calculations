"""Clip a raster by a polygon vector."""
import argparse
import math
import tempfile
import os
import shutil

import logging
import sys

from ecoshard import geoprocessing
from osgeo import gdal
from osgeo import osr
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
        'Clip a raster by a polygon vector. Result is a clipped version of '
        'input in the same projection as the input raster with  size equal to '
        'the projected bounding box of the input vector and any areas outside '
        'of the polygon masked out. Target raster will be named '
        '{base raster name}_clipped_by_{base vector name}.tif'))
    parser.add_argument(
        'input_raster', help='Path to arbitrary input raster')
    parser.add_argument(
        'vector_to_clip_with', help='Path to arbitrary vector to clip')
    args = parser.parse_args()

    temp_dir = tempfile.mkdtemp(dir=os.getcwd(), prefix='clip_raster_workspace')

    raster_info = geoprocessing.get_raster_info(args.input_raster)
    raster_projection_wkt = raster_info['projection_wkt']

    projected_vector_path = os.path.join(
        temp_dir, os.path.basename(args.vector_to_clip_with))

    LOGGER.info(f'reproject vector to {projected_vector_path}')
    geoprocessing.reproject_vector(
        args.vector_to_clip_with, raster_projection_wkt, projected_vector_path,
        driver_name='GPKG')

    projected_vector_info = geoprocessing.get_vector_info(projected_vector_path)

    target_bb = geoprocessing.merge_bounding_box_list(
        [raster_info['bounding_box'], projected_vector_info['bounding_box']],
        'intersection')

    raster_basename = os.path.basename(os.path.splitext(
        args.input_raster)[0])
    vector_basename = os.path.basename(os.path.splitext(
        args.vector_to_clip_with)[0])
    clip_raster_path = os.path.join(
        os.getcwd(), f'{raster_basename}_clipped_by_{vector_basename}.tif')
    LOGGER.info(f'clipping to {clip_raster_path}')
    geoprocessing.warp_raster(
        args.input_raster, raster_info['pixel_size'], clip_raster_path,
        'near', target_bb=target_bb,
        vector_mask_options={'mask_vector_path': projected_vector_path},
        working_dir=os.getcwd())
    LOGGER.info(f'all done, raster at {clip_raster_path}')
    shutil.rmtree(temp_dir)
