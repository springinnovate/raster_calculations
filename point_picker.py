"""Record pixel values where points overlap the raster."""
import argparse
import glob
import os

from osgeo import gdal
from osgeo import ogr
from osgeo import osr


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pixel picker.')
    parser.add_argument('raster_path', help='path to sample raster')
    parser.add_argument('point_vector_path', help='path to point vector')
    args = parser.parse_args()

    point_vector = gdal.OpenEx(args.point_vector_path, gdal.OF_VECTOR)
    point_layer = point_vector.GetLayer()
    point_srs = point_layer.GetSpatialRef()

    dirname = os.path.basename(os.path.dirname(args.raster_path))

    gpkg_driver = ogr.GetDriverByName('GPKG')
    target_vector_path = '%s.gpkg' % dirname
    if os.path.exists(target_vector_path):
        os.remove(target_vector_path)
    target_vector = gpkg_driver.CreateDataSource(target_vector_path)
    target_layer = target_vector.CreateLayer(
        dirname, point_srs, ogr.wkbPoint)

    base_layer_defn = point_layer.GetLayerDefn()
    base_layer_field_name_list = []
    for index in range(base_layer_defn.GetFieldCount()):
        field_defn = base_layer_defn.GetFieldDefn(index)
        base_layer_field_name_list.append(field_defn.GetName())
        target_layer.CreateField(field_defn)

    for raster_path in glob.glob(args.raster_path):
        basename = os.path.splitext(os.path.basename(raster_path))[0]
        target_layer.CreateField(ogr.FieldDefn(basename, ogr.OFTReal))

    fid_to_feature_map = {}

    for raster_path in glob.glob(args.raster_path):
        basename = os.path.splitext(os.path.basename(raster_path))[0]
        raster = gdal.OpenEx(raster_path, gdal.OF_RASTER)
        band = raster.GetRasterBand(1)
        raster_srs_wkt = raster.GetProjection()
        raster_srs = osr.SpatialReference()
        raster_srs.ImportFromWkt(raster_srs_wkt)

        gt = raster.GetGeoTransform()
        inv_gt = gdal.InvGeoTransform(gt)

        point_to_raster_transform = osr.CoordinateTransformation(
            point_srs, raster_srs)
        print(raster_path)
        point_layer.ResetReading()
        for point in point_layer:
            point_fid = point.GetFID()
            point_geom = point.GetGeometryRef()
            base_point_geom = point_geom.Clone()
            val = point_geom.Transform(point_to_raster_transform)
            result = gdal.ApplyGeoTransform(
                inv_gt, point_geom.GetX(), point_geom.GetY())
            if (result[0] >= 0 and result[0] < raster.RasterXSize and
                    result[1] >= 0 and result[1] < raster.RasterYSize):
                print('its in range')
                if point_fid not in fid_to_feature_map:
                    feature_defn = target_layer.GetLayerDefn()
                    feature = ogr.Feature(feature_defn)
                    feature.SetGeometry(base_point_geom.Clone())
                    for base_field_name in base_layer_field_name_list:
                        feature.SetField(
                            base_field_name, point.GetField(base_field_name))
                    fid_to_feature_map[point_fid] = feature
                pixel_value = band.ReadAsArray(
                    result[0], result[1], 1, 1)[0][0]
                fid_to_feature_map[point_fid].SetField(
                    basename, float(pixel_value))

    for feature in fid_to_feature_map.values():
        target_layer.CreateFeature(feature)
