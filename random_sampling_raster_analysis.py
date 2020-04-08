"""Multi field polygon raster analysis for Rob."""
import logging
import os
import multiprocessing
import sys

from osgeo import gdal
from osgeo import ogr
from osgeo import osr
import pygeoprocessing
import numpy
import shapely
import shapely.geometry
import shapely.wkb
import taskgraph

gdal.SetCacheMax(2**28)

WORKSPACE_DIR = 'workspace_sample_points'
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.INFO)


def rasterize_like(
        base_raster_path, base_vector_path, fieldname,
        target_raster_type, target_nodata, target_raster_path):
    """Rasterize `fieldname` to a new raster same size as `base_raster_path`"""
    pygeoprocessing.new_raster_from_base(
        base_raster_path, target_raster_path, target_raster_type,
        [target_nodata])
    pygeoprocessing.rasterize(
        base_vector_path, target_raster_path,
        option_list=['ATTRIBUTE=%s' % fieldname])


def robs_function(
        base_array, RES1, RES2, RES3A, RES3B, RES3C, RES3D, RES3E, RES3F,
        RES4, RES5, RES6, COM1, COM2, COM3, COM4, COM5, COM6,
        COM7, COM8, COM9, COM10, IND1, IND2, IND3, IND4,
        IND5, IND6, AGR1, REL1, GOV1, GOV2, EDU1, EDU2, nodata):
    """Rob you should write your function here."""
    result = numpy.empty_like(base_array)
    result[:] = nodata
    valid_mask = (
        ~numpy.isclose(base_array, nodata) &
        ~numpy.isclose(RES1, nodata) &
        ~numpy.isclose(RES2, nodata) &
        ~numpy.isclose(RES3A, nodata) &
        ~numpy.isclose(RES3B, nodata) &
        ~numpy.isclose(RES3C, nodata) &
        ~numpy.isclose(RES3D, nodata) &
        ~numpy.isclose(RES3E, nodata) &
        ~numpy.isclose(RES3F, nodata) &
        ~numpy.isclose(RES4, nodata) &
        ~numpy.isclose(RES5, nodata) &
        ~numpy.isclose(RES6, nodata) &
        ~numpy.isclose(COM1, nodata) &
        ~numpy.isclose(COM2, nodata) &
        ~numpy.isclose(COM3, nodata) &
        ~numpy.isclose(COM4, nodata) &
        ~numpy.isclose(COM5, nodata) &
        ~numpy.isclose(COM6, nodata) &
        ~numpy.isclose(COM7, nodata) &
        ~numpy.isclose(COM8, nodata) &
        ~numpy.isclose(COM9, nodata) &
        ~numpy.isclose(COM10, nodata) &
        ~numpy.isclose(IND1, nodata) &
        ~numpy.isclose(IND2, nodata) &
        ~numpy.isclose(IND3, nodata) &
        ~numpy.isclose(IND4, nodata) &
        ~numpy.isclose(IND5, nodata) &
        ~numpy.isclose(IND6, nodata) &
        ~numpy.isclose(AGR1, nodata) &
        ~numpy.isclose(REL1, nodata) &
        ~numpy.isclose(GOV1, nodata) &
        ~numpy.isclose(GOV2, nodata) &
        ~numpy.isclose(EDU1, nodata) &
        ~numpy.isclose(EDU2, nodata))

    result[valid_mask] = (
        base_array[valid_mask] * RES1[valid_mask] * RES2[valid_mask] *
        RES3A[valid_mask] * RES3B[valid_mask] * RES3C[valid_mask] *
        RES3D[valid_mask] * RES3E[valid_mask] * RES3F[valid_mask] *
        RES4[valid_mask] * RES5[valid_mask] * RES6[valid_mask] *
        COM1[valid_mask] * COM2[valid_mask] * COM3[valid_mask] *
        COM4[valid_mask] * COM5[valid_mask] * COM6[valid_mask] *
        COM7[valid_mask] * COM8[valid_mask] * COM9[valid_mask] *
        COM10[valid_mask] * IND1[valid_mask] * IND2[valid_mask] *
        IND3[valid_mask] * IND4[valid_mask] * IND5[valid_mask] *
        IND6[valid_mask] * AGR1[valid_mask] * REL1[valid_mask] *
        GOV1[valid_mask] * GOV2[valid_mask] * EDU1[valid_mask] *
        EDU2[valid_mask])
    return result


if __name__ == "__main__":
    for dir_path in [WORKSPACE_DIR, CHURN_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    task_graph = taskgraph.TaskGraph(CHURN_DIR, -1)

    vector_path = 'data/Exposure4OLU2.shp'
    raster_path = 'data/olu_00000-0-0-000-000000-00000-00-00000-00_depth.tif'
    raster_info = pygeoprocessing.get_raster_info(raster_path)
    raster_projection = raster_info['projection']
    base_nodata = raster_info['nodata'][0]

    reprojected_vector_path = os.path.join(
        CHURN_DIR, 'reprojected_' + os.path.basename(vector_path))

    reproject_task = task_graph.add_task(
        func=pygeoprocessing.reproject_vector,
        args=(vector_path, raster_projection, reprojected_vector_path),
        target_path_list=[reprojected_vector_path],
        task_name='project vector to raster')

    fields_to_process = [
        'RES1', 'RES2', 'RES3A', 'RES3B', 'RES3C', 'RES3D', 'RES3E', 'RES3F',
        'RES4', 'RES5', 'RES6', 'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6',
        'COM7', 'COM8', 'COM9', 'COM10', 'IND1', 'IND2', 'IND3', 'IND4',
        'IND5', 'IND6', 'AGR1', 'REL1', 'GOV1', 'GOV2', 'EDU1', 'EDU2']

    gpkg_driver = ogr.GetDriverByName('GPKG')
    point_vector_path = os.path.join(CHURN_DIR, 'point_sample.gpkg')
    point_vector = gpkg_driver.CreateDataSource(
        point_vector_path)
    projection_srs = osr.SpatialReference()
    projection_srs.ImportFromWkt(raster_projection)
    point_layer = point_vector.CreateLayer(
        'point_sample', projection_srs, ogr.wkbPoint)

    # wait for vector to reproject
    task_graph.close()
    task_graph.join()

    geotransform = raster_info['geotransform']
    n_cols, n_rows = raster_info['raster_size']
    inv_geotransform = gdal.InvGeoTransform(geotransform)

    vector = gdal.OpenEx(reprojected_vector_path, gdal.OF_VECTOR)
    layer = vector.GetLayer()
    n_features = layer.GetFeatureCount()

    layer_defn = layer.GetLayerDefn()
    original_field_name_list = []
    for i in range(layer_defn.GetFieldCount()):
        point_layer.CreateField(layer_defn.GetFieldDefn(i))
        original_field_name_list.append(layer_defn.GetFieldDefn(i).GetName())
    point_layer.CreateField(ogr.FieldDefn("rasterval", ogr.OFTReal))

    point_layer_def = point_layer.GetLayerDefn()
    point_layer.StartTransaction()

    raster = gdal.OpenEx(raster_path, gdal.OF_RASTER | gdal.GA_ReadOnly)
    band = raster.GetRasterBand(1)

    for index, poly_feature in enumerate(layer):
        if index % 100 == 0:
            print('%2.f%% complete' % (100*index/n_features))
        geometry = poly_feature.GetGeometryRef()
        geom_shapely = shapely.wkb.loads(geometry.ExportToWkb())
        geom_prep = shapely.prepared.prep(geom_shapely)
        n = 0
        x_min = geom_shapely.bounds[0]
        x_width = geom_shapely.bounds[2]-geom_shapely.bounds[0]
        y_min = geom_shapely.bounds[1]
        y_width = geom_shapely.bounds[3]-geom_shapely.bounds[1]
        while n < 100:
            xp, yp = numpy.random.random(2)
            point_geom = shapely.geometry.Point(
                x_min+x_width*xp, y_min+y_width*yp)
            if geom_prep.intersects(point_geom):
                n += 1
                raster_x, raster_y = [
                    int(v) for v in gdal.ApplyGeoTransform(
                        inv_geotransform, point_geom.x, point_geom.y)]
                if (raster_x < 0 or raster_y < 0 or
                        raster_x >= n_cols or raster_y >= n_rows):
                    continue
                raster_val = band.ReadAsArray(raster_x, raster_y, 1, 1)[0, 0]
                if (numpy.isclose(raster_val, base_nodata) or
                        numpy.isnan(raster_val)):
                    continue
                print('%f %d %d' % (raster_val, raster_x, raster_y))
                geom = ogr.CreateGeometryFromWkb(point_geom.wkb)
                point_feature = ogr.Feature(point_layer_def)
                point_feature.SetGeometry(geom)
                point_feature.SetField('rasterval', raster_val)
                for poly_field_name in original_field_name_list:
                    point_feature.SetField(
                        poly_field_name,
                        poly_feature.GetField(poly_field_name))
                point_layer.CreateFeature(point_feature)
                print('made it')
                point_layer.CommitTransaction()
                sys.exit()
