"""Demo of how to use pandas to multiply one table by another."""
from osgeo import gdal
import pandas

# Justin said this was his reference
esa_lulc_geotransform = (-180.0, 0.0027777778, 0.0, 90.0, 0.0, -0.0027777778)
esa_n_cols = 129600
inv_gt = gdal.InvGeoTransform(esa_lulc_geotransform)

base_table_path = r"C:\Users\Becky\Dropbox\SPRING\spring_projects\unilever\1_train_data_convall1s.csv"
lasso_table_path = r"C:\Users\Becky\Dropbox\SPRING\spring_projects\unilever\lasso_interacted_not_forest_gs1to100_alpha0-01_params.csv"

print('reading')
base_table_df = pandas.read_csv(base_table_path)
lasso_df = pandas.read_csv(lasso_table_path)

index_list = []
column_list = []

target_df = pandas.DataFrame()
for _, row in lasso_df.iterrows():
    header = row[0]
    index_list.append(header)

    print(header)

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
    # the "2:" don't include the coordates that are the first two columns
    predicted_col.append(sum(row[:]))

target_df['predicted'] = predicted_col

print('writing')
target_df.to_csv('result.csv', index=False)