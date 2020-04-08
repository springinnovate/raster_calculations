"""Multi field polygon raster analysis for Rob."""
import logging
import os
import multiprocessing
import sys
import time

from osgeo import gdal
from osgeo import ogr
from osgeo import osr
import pygeoprocessing
import numpy
import shapely
import shapely.geometry
import shapely.wkb
import taskgraph

gdal.SetCacheMax(2**26)

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


def write_results(
        result_queue, feature_count, base_vector_path, point_vector_path):
    vector = gdal.OpenEx(base_vector_path, gdal.OF_VECTOR)
    poly_layer = vector.GetLayer()
    layer_defn = poly_layer.GetLayerDefn()

    point_vector = gdal.OpenEx(
        point_vector_path, gdal.OF_VECTOR | gdal.GA_Update)
    point_layer = point_vector.GetLayer()

    original_field_name_list = []
    for i in range(layer_defn.GetFieldCount()):
        point_layer.CreateField(layer_defn.GetFieldDefn(i))
        original_field_name_list.append(layer_defn.GetFieldDefn(i).GetName())
    point_layer.CreateField(ogr.FieldDefn("rasterval", ogr.OFTReal))

    point_layer_def = point_layer.GetLayerDefn()

    n_written = 0
    point_layer.StartTransaction()
    while True:
        payload = result_queue.get()
        if payload == 'STOP':
            print('write results STOP')
            break

        if n_written % 100 == 0:
            point_layer.CommitTransaction()
            point_layer.StartTransaction()
            print(
                'no less than %.2f%% complete' %
                (n_written / feature_count * 100.0))
        n_written += 1
        feature_id, raster_val_list, point_geom_wkb_list = payload
        poly_feature = poly_layer.GetFeature(feature_id)

        for raster_val, point_geom_wkb in zip(
                raster_val_list, point_geom_wkb_list):
            geom = ogr.CreateGeometryFromWkb(point_geom_wkb)
            point_feature = ogr.Feature(point_layer_def)
            point_feature.SetGeometry(geom)
            point_feature.SetField('rasterval', raster_val)
            for poly_field_name in original_field_name_list:
                point_feature.SetField(
                    poly_field_name,
                    poly_feature.GetField(poly_field_name))
            point_layer.CreateFeature(point_feature)
    point_layer.CommitTransaction()
    poly_layer = None
    vector = None
    point_layer = None
    point_vector = None


def poly_test_worker(work_queue, raster_path, poly_vector_path, result_queue):

    raster_info = pygeoprocessing.get_raster_info(raster_path)
    base_nodata = raster_info['nodata']
    geotransform = raster_info['geotransform']
    n_cols, n_rows = raster_info['raster_size']
    inv_geotransform = gdal.InvGeoTransform(geotransform)

    raster = gdal.OpenEx(raster_path, gdal.OF_RASTER | gdal.GA_ReadOnly)
    band = raster.GetRasterBand(1)

    poly_vector = gdal.OpenEx(
        poly_vector_path, gdal.OF_VECTOR | gdal.GA_ReadOnly)
    poly_layer = poly_vector.GetLayer()

    while True:
        payload = work_queue.get()
        if payload == 'STOP':
            work_queue.put('STOP')
            print('poly test STOP')
            break
        poly_fid = payload

        poly_feature = poly_layer.GetFeature(poly_fid)

        geometry = poly_feature.GetGeometryRef()
        geom_shapely = shapely.wkb.loads(geometry.ExportToWkb())
        geom_prep = shapely.prepared.prep(geom_shapely)
        n = 0
        x_min = geom_shapely.bounds[0]
        x_width = geom_shapely.bounds[2]-geom_shapely.bounds[0]
        y_min = geom_shapely.bounds[1]
        y_width = geom_shapely.bounds[3]-geom_shapely.bounds[1]
        raster_val_list = []
        point_geom_wkb_list = []
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
                if (numpy.isclose(raster_val, base_nodata)):
                    continue
                raster_val_list.append(raster_val)
                point_geom_wkb_list.append(point_geom.wkb)
        if raster_val_list:
            result_queue.put(
                (poly_fid, raster_val_list, point_geom_wkb_list))


if __name__ == "__main__":
    for dir_path in [WORKSPACE_DIR, CHURN_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    start_time = time.time()
    task_graph = taskgraph.TaskGraph(CHURN_DIR, -1)

    result_queue = multiprocessing.Queue()
    work_queue = multiprocessing.Queue()

    vector_path = 'data/Exposure4OLU2.shp'
    raster_path = 'data/olu_00000-0-0-000-000000-00000-00-00000-00_depth.tif'
    raster_info = pygeoprocessing.get_raster_info(raster_path)
    raster_projection = raster_info['projection']

    reprojected_vector_path = os.path.join(
        CHURN_DIR, 'reprojected_' + os.path.basename(vector_path))

    reproject_task = task_graph.add_task(
        func=pygeoprocessing.reproject_vector,
        args=(vector_path, raster_projection, reprojected_vector_path),
        target_path_list=[reprojected_vector_path],
        task_name='project vector to raster')

    gpkg_driver = ogr.GetDriverByName('GPKG')
    point_vector_path = os.path.join(CHURN_DIR, 'point_sample.gpkg')
    point_vector = gpkg_driver.CreateDataSource(
        point_vector_path)
    projection_srs = osr.SpatialReference()
    projection_srs.ImportFromWkt(raster_projection)
    point_layer = point_vector.CreateLayer(
        'point_sample', projection_srs, ogr.wkbPoint)
    point_layer = None
    point_vector = None

    # wait for vector to reproject
    task_graph.close()
    task_graph.join()

    vector = gdal.OpenEx(
        reprojected_vector_path, gdal.OF_VECTOR | gdal.GA_ReadOnly)
    layer = vector.GetLayer()
    feature_count = layer.GetFeatureCount()

    write_worker = multiprocessing.Process(
        target=write_results,
        args=(
            result_queue, feature_count, reprojected_vector_path,
            point_vector_path))
    write_worker.start()

    test_worker_list = []
    for _ in range(multiprocessing.cpu_count()//2):
        test_worker_process = multiprocessing.Process(
            target=poly_test_worker,
            args=(
                work_queue, raster_path, reprojected_vector_path,
                result_queue))
        test_worker_process.start()
        test_worker_list.append(test_worker_process)

    for poly_feature in layer:
        work_queue.put(poly_feature.GetFID())
    work_queue.put('STOP')

    for test_worker in test_worker_list:
        test_worker.join()
    result_queue.put('STOP')
    write_worker.join()

    print('ran in %.2fs' % (time.time()-start_time))
