"""Demo of how to use pandas to multiply one table by another."""
import argparse
import os
import multiprocessing

from osgeo import gdal
from osgeo import ogr
from osgeo import osr
import pandas
import pygeoprocessing
import taskgraph

gdal.SetCacheMax(2**30)

NCPUS = multiprocessing.cpu_count()

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

    args = parser.parse_args()
    # Justin said this was his reference

    lasso_table_path = args.lasso_table_path

    print('reading')
    base_table_df = pandas.read_csv(base_table_path)
    lasso_df = pandas.read_csv(lasso_table_path)

    target_df = pandas.DataFrame()

    if args.point_cloud:
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

        if args.point_cloud:
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
        if '^' in header:
            col, power = header.split('^')
            target_df[header] = base_table_df[col]**int(power)*lasso_val

    # Add a "predicted" column
    predicted_col = []
    for _, row in target_df.iterrows():
        # the "2:" don't include the coordates that are the first two columns
        predicted_col.append(sum(row[2:]))

    target_df['predicted'] = predicted_col
    header_pos['predicted'] = len(header_pos)

    print('writing')


def raster_model(*raster_nodata_term_order_list):
    """Calculate summation of terms accounting for

    Args:
        raster_nodata_term_order_list (list): a series of 4 elements for each
            raster in the order:
                * raster array slice
                * raster nodata
                * constant float term to multiply to this raster
                * integer (n) order term to exponentiate the raster (r) (r**n)
                * list of additional raster indexes to multiply this raster by

    Returns:
        each term evaluated, then the sum of all the terms.
    """
    pass
