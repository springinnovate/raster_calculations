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

gdal.UseExceptions()

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


def simplify_poly(
        base_vector_path, target_epsg, target_tolerance,
        target_simplified_vector_path):
    """Simplify base to target."""
    target_srs = osr.SpatialReference()
    target_srs.ImportFromEPSG(int(target_epsg))
    geoprocessing.reproject_vector(
        base_vector_path, target_srs.ExportToWkt(),
        target_simplified_vector_path,
        driver_name='GPKG',
        copy_fields=True,
        geometry_type=ogr.wkbMultiPolygon,
        simplify_tol=target_tolerance)


def rasterize_id_by_value(vector_path, field_id, target_raster_path):
    """Rasterize a subset of vector onto raster.

    Args:
        vector_path (str): path to existing vector
        target_raster_path (str): path to existing raster
        field_id (str): field index in `vector_path` to reference
    Returns:
        None
    """
    option_list = ["MERGE_ALG=REPLACE", "ALL_TOUCHED=TRUE"]
    burn_values = None
    if field_id is not None:
        option_list += f"ATTRIBUTE={field_id}"
    else:
        burn_values = [1]
    geoprocessing.rasterize(
        vector_path, target_raster_path, burn_values=burn_values,
        option_list=option_list)


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
    parser.add_argument('vector_path_pattern', type=str, help='Path or pattern to vectors to rasterize.')
    parser.add_argument('--field_to_rasterize', required=False, type=str, help='Field in vector to rasterize, should be numeric.')
    parser.add_argument('--target_pixel_size', required=True, type=float, help='Desired target pixel size in target projection.')
    parser.add_argument('--raster_data_type', required=True, type=str, help='Target data type either `byte`, `int`, or `float`.')
    parser.add_argument('--raster_nodata_value', required=True, type=float, help='Target nodata value.')
    parser.add_argument('--target_raster_path', required=True, help='Desired path to created file.')
    parser.add_argument('--target_epsg', required=True, help='Target EPSG projection code.')
    args = parser.parse_args()

    if args.raster_data_type == 'int':
        target_data_type = gdal.GDT_Int32
    elif args.raster_data_type == 'float':
        target_data_type = gdal.GDT_Float32
    elif args.raster_data_type == 'byte':
        target_data_type = gdal.GDT_Byte
    target_dirname = os.path.dirname(args.target_raster_path)
    if target_dirname:
        os.makedirs(target_dirname, exist_ok=True)
    vector_path_list = list(glob.glob(args.vector_path_pattern))
    task_graph = taskgraph.TaskGraph(
        target_dirname, min(os.cpu_count(), len(vector_path_list)))

    simplified_vector_path_list = []
    for vector_path in vector_path_list:
        vector_basename = os.path.basename(os.path.splitext(vector_path)[0])
        LOGGER.info(f'processing {vector_path}')
        simplified_vector_path = os.path.join(
            target_dirname, f'_{vector_basename}_simple.gpkg')
        _ = task_graph.add_task(
            func=simplify_poly,
            args=(
                vector_path, args.target_epsg, args.target_pixel_size/2,
                simplified_vector_path),
            ignore_path_list=[vector_path],
            target_path_list=[simplified_vector_path],
            task_name=f'simplifying {simplified_vector_path}')
        simplified_vector_path_list.append(simplified_vector_path)

    task_graph.join()

    bounding_box_list = []
    for simplified_vector_path in simplified_vector_path_list:
        vector_info = geoprocessing.get_vector_info(simplified_vector_path)
        bounding_box_list.append(vector_info['bounding_box'])

    target_bounding_box = geoprocessing.merge_bounding_box_list(
        bounding_box_list, 'union')
    LOGGER.info(f'creating raster at {args.target_raster_path}')
    create_raster_from_bounds(
        target_bounding_box, target_data_type, args.target_pixel_size,
        args.raster_nodata_value, args.target_epsg, args.target_raster_path)
    for simplified_vector_path in simplified_vector_path_list:
        rasterize_id_by_value(
            simplified_vector_path,
            args.field_to_rasterize,
            args.target_raster_path,)

    task_graph.close()
    task_graph.join()


def create_raster_from_bounds(
        bounding_box, target_pixel_type, target_pixel_length, target_nodata,
        target_epsg,
        target_raster_path):
    target_pixel_size = (target_pixel_length, -target_pixel_length)
    # Determine the width and height of the tiff in pixels based on the
    # maximum size of the combined envelope of all the features
    xwidth = numpy.subtract(*[bounding_box[i] for i in (2, 0)])
    ywidth = numpy.subtract(*[bounding_box[i] for i in (3, 1)])
    if numpy.isclose(xwidth, 0) and numpy.isclose(ywidth, 0):
        raise ValueError(
            f'bounding box appears to be empty {bounding_box} suggesting '
            f'vector has no geometry')
    n_cols = abs(round(xwidth / target_pixel_size[0]))
    n_rows = abs(round(ywidth / target_pixel_size[1]))
    n_cols = max(1, n_cols)
    n_rows = max(1, n_rows)

    driver = gdal.GetDriverByName('GTIFF')
    n_bands = 1
    raster = driver.Create(
        target_raster_path, n_cols, n_rows, n_bands, target_pixel_type,
        options=(
            'TILED=YES', 'BIGTIFF=YES', 'COMPRESS=LZW',
            'BLOCKXSIZE=256', 'BLOCKYSIZE=256', 'NUM_THREADS=ALL_CPUS'))
    raster.GetRasterBand(1).SetNoDataValue(target_nodata)

    # Set the transform based on the upper left corner and given pixel
    # dimensions
    x_source = bounding_box[0]
    y_source = bounding_box[3]
    raster_transform = [
        x_source, target_pixel_size[0], 0.0,
        y_source, 0.0, target_pixel_size[1]]
    raster.SetGeoTransform(raster_transform)

    target_srs = osr.SpatialReference()
    target_srs.ImportFromEPSG(int(target_epsg))
    raster.SetProjection(target_srs.ExportToWkt())


if __name__ == '__main__':
    main()
