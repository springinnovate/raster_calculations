"""Rasterize the landuse polygons onto a single raster."""
import argparse
import glob
import logging
import os
import subprocess
import sys

from ecoshard import geoprocessing
from ecoshard import taskgraph
from osgeo import gdal
from osgeo import ogr
from osgeo import osr
import numpy

from ecoshard.geoprocessing.geoprocessing_core import DEFAULT_GTIFF_CREATION_TUPLE_OPTIONS

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
logging.getLogger('taskgraph').setLevel(logging.INFO)
LOGGER = logging.getLogger(__name__)


def _get_centroid(vector_path):
    """Return x/y centroid of the entire vector."""
    vector = ogr.Open(vector_path)
    layer = vector.GetLayer()
    point_cloud = ogr.Geometry(type=ogr.wkbMultiPoint)
    for feature in layer:
        geom = feature.GetGeometryRef()
        point_cloud.AddGeometry(geom.Centroid())
        geom = None
        feature = None
    layer = None
    vector = None
    centroid = point_cloud.Centroid()
    return centroid


def simplify_poly(base_vector_path, target_vector_path, tol):
    """Simplify base to target."""
    vector = ogr.Open(base_vector_path)
    layer = vector.GetLayer()
    target_srs = osr.SpatialReference()
    target_srs.ImportFromEPSG(4326)
    geoprocessing.reproject_vector(
        base_vector_path, target_srs.ExportToWkt(), target_vector_path,
        driver_name='GPKG',
        copy_fields=True,
        geometry_type=ogr.wkbMultiPolygon,
        simplify_tol=tol)


def rasterize_id_by_value(vector_path, raster_path, field_id):
    """Rasterize a subset of vector onto raster.

    Args:
        vector_path (str): path to existing vector
        raster_path (str): path to existing raster
        field_id (str): field index in `vector_path` to reference
    Returns:
        None
    """
    geoprocessing.rasterize(
        vector_path, raster_path,
        option_list=["MERGE_ALG=REPLACE", "ALL_TOUCHED=TRUE", f"ATTRIBUTE={field_id}"])


def get_all_field_values(shapefile_path, field_id):
    """Return all values in field_id for all features in ``shapefile_path``."""
    landcover_id_set = set()
    vector = gdal.OpenEx(shapefile_path, gdal.OF_VECTOR)
    layer = vector.GetLayer()
    for feature in layer:
        landcover_id_set.add(feature.GetField(field_id))
    return landcover_id_set


def main():
    parser = argparse.ArgumentParser(description='Rasterize a polygon to a new raster.')
    parser.add_argument('vector_path', type=str, help='Path to vector to rasterize.')
    parser.add_argument('--field_to_rasterize', required=True, type=str, help='Field in vector to rasterize, should be numeric.')
    parser.add_argument('--target_pixel_size', required=True, type=float, help='Desired target pixel size in units of the input vector')
    parser.add_argument('--raster_data_type', required=True, type=str, help='either `byte`, `int`, or `float`')
    parser.add_argument('--raster_nodata_value', required=True, type=float, help='Target nodata value.')
    parser.add_argument('--target_raster_path', required=True, help='Desired path to created file.')
    args = parser.parse_args()

    if args.raster_data_type == 'int':
        target_data_type = gdal.GDT_Int32
    elif args.raster_data_type == 'float':
        target_data_type = gdal.GDT_Float32
    elif args.raster_data_type == 'byte':
        target_data_type = gdal.GDT_Byte

    vector_basename = os.path.basename(os.path.splitext(args.vector_path)[0])
    if args.target_raster_path is not None:
        target_raster_path = args.target_raster_path
    else:
        target_raster_path = f'{vector_basename}_{args.field_to_rasterize}.tif'

    target_dirname = os.path.dirname(target_raster_path)
    if target_dirname:
        os.makedirs(target_dirname, exist_ok=True)

    task_graph = taskgraph.TaskGraph('.', -1)

    bounding_box_list = []
    LOGGER.info(f'processing {args.vector_path}')
    simplified_vector_path = os.path.join(
        target_dirname, f'_{vector_basename}_simple.gpkg')

    simplify_task = task_graph.add_task(
        func=simplify_poly,
        args=(args.vector_path, simplified_vector_path, args.target_pixel_size/2),
        ignore_path_list=[args.vector_path],
        target_path_list=[simplified_vector_path],
        task_name=f'simplifying {simplified_vector_path}')
    simplify_task.join()
    task_graph.close()
    task_graph.join()

    vector_info = geoprocessing.get_vector_info(simplified_vector_path)
    LOGGER.info(f'creating raster at {target_raster_path}')

    geoprocessing.create_raster_from_vector_extents(
        simplified_vector_path, target_raster_path,
        (args.target_pixel_size, -args.target_pixel_size),
        target_data_type, args.raster_nodata_value)

    rasterize_id_by_value(
        simplified_vector_path, target_raster_path, args.field_to_rasterize)


if __name__ == '__main__':
    main()
