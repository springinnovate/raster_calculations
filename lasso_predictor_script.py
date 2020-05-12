"""Demo of how to use pandas to multiply one table by another."""
import os

from osgeo import gdal
from osgeo import ogr
from osgeo import osr
import pandas

# Justin said this was his reference
esa_lulc_geotransform = (-180.0, 30*0.0027777778, 0.0, 90.0, 0.0, 30*-0.0027777778)
esa_n_cols = 129600 // 30
inv_gt = gdal.InvGeoTransform(esa_lulc_geotransform)

base_table_path = '1_test_data.csv'
lasso_table_path = 'lasso_interacted_not_forest_gs1to100_params.csv'

print('reading')
base_table_df = pandas.read_csv(base_table_path)
lasso_df = pandas.read_csv(lasso_table_path)

target_df = pandas.DataFrame()

gpkg_driver = ogr.GetDriverByName('GPKG')
base_id = os.path.basename(os.path.splitext(base_table_path)[0])
vector = gpkg_driver.CreateDataSource(f'{base_id}.gpkg')
wgs84_srs = osr.SpatialReference()
wgs84_srs.ImportFromEPSG(4326)
layer = vector.CreateLayer(base_id, wgs84_srs, geom_type=ogr.wkbPoint)
layer_def = layer.GetLayerDefn()

header_pos = {}
print('start lasso')
for row_index, row in lasso_df.iterrows():
    header = row[0]
    header_pos[header] = row_index

    field = ogr.FieldDefn(header, ogr.OFTReal)
    layer.CreateField(field)

    lasso_val = row[1]
    if header in base_table_df:
        # modify that column
        target_df[header] = base_table_df[header]*lasso_val
    if '*' in header:
        # add a new column
        col_a, col_b = header.split('*')
        target_df[header] = (
            base_table_df[col_a] * base_table_df[col_b] * row[1])

# Add a "predicted" column
predicted_col = []
for _, row in target_df.iterrows():
    predicted_col.append(sum(row[:]))

target_df['predicted'] = predicted_col
field = ogr.FieldDefn('predicted', ogr.OFTReal)
layer.CreateField(field)
header_pos['predicted'] = len(header_pos)

layer.StartTransaction()
print('generate point cloud')
for row_index, row in base_table_df.iterrows():
    positional_index = row['positional_index']
    pos_x = positional_index % esa_n_cols
    pos_y = positional_index // esa_n_cols
    lng, lat = gdal.ApplyGeoTransform(esa_lulc_geotransform, pos_x, pos_y)
    point_geom = ogr.Geometry(ogr.wkbPoint)
    point_geom.AddPoint(lng, lat)

    point_feature = ogr.Feature(layer_def)
    point_feature.SetGeometry(point_geom)

    for header, header_col in header_pos.items():
        point_feature.SetField(
            header, target_df.iloc[row_index, header_col])
    layer.CreateFeature(point_feature)
layer.CommitTransaction()

print('writing')
target_df.to_csv('result.csv', index=False)
