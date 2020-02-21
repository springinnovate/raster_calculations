"""Count non-zero non-nodata pixels in raster, get percentile values, and sum above each percentile to build the CDF."""
import datetime
import sys
import os
import logging
import ecoshard
import subprocess

import matplotlib.pyplot
import numpy
import scipy.interpolate
import pygeoprocessing
from osgeo import gdal
from osgeo import osr
from osgeo import ogr
import taskgraph

gdal.SetCacheMax(2**30)

BUCKET_PATTERN = 'gs://shared-with-users/realized_services/*.tif'
#raster_path = r"C:\Users\Becky\Documents\raster_calculations\normalized_realized_grazing_md5_d03b_resample_30x.tif"
#raster_path = r"C:\Users\rpsharp\Downloads\normalized_realized_grazing_md5_d03b584dac965539a77bf96cba3f8096.tif"
WORKSPACE_DIR = 'cdf_by_country'
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')
NCPUS = -1
for dir_path in [WORKSPACE_DIR, CHURN_DIR]:
    try:
        os.makedirs(dir_path)
    except OSError:
        pass

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
logging.getLogger('matplotlib').setLevel(logging.ERROR)

WORLD_BORDERS_URL = 'https://storage.googleapis.com/critical-natural-capital-ecoshards/countries_iso3_md5_6fb2431e911401992e6e56ddf0a9bcda.gpkg'
COUNTRY_WORKSPACES = os.path.join(WORKSPACE_DIR, 'country_workspaces')

PERCENTILE_LIST = list(range(0, 101, 5))


def main():
    """Entry point."""
    for dir_path in [WORKSPACE_DIR, COUNTRY_WORKSPACES]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, -1, 5.0)
    world_borders_path = os.path.join(
        WORKSPACE_DIR, os.path.basename(WORLD_BORDERS_URL))
    download_task = task_graph.add_task(
        func=ecoshard.download_url,
        args=(WORLD_BORDERS_URL, world_borders_path),
        target_path_list=[world_borders_path],
        task_name='download world borders')

    download_task.join()

    world_borders_vector = gdal.OpenEx(world_borders_path, gdal.OF_VECTOR)
    world_borders_layer = world_borders_vector.GetLayer()

    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)

    result = subprocess.run('gsutil ls -p ecoshard %s' % BUCKET_PATTERN, capture_output=True, shell=True, check=True)
    for gs_path in [x.decode('utf-8') for x in result.stdout.splitlines()]:
        print(gs_path)
        raster_id = os.path.basename(os.path.splitext(gs_path)[0])
        raster_path = os.path.join(CHURN_DIR, os.path.basename(gs_path))
        print('copying %s to %s' % (gs_path, raster_path))
        subprocess.run('gsutil cp %s %s' % (gs_path, raster_path), shell=True, check=True)
        raster_info = pygeoprocessing.get_raster_info(raster_path)

        country_threshold_table_path = os.path.join(
            WORKSPACE_DIR, 'country_threshold_%s.csv' % raster_id)
        country_threshold_table_file = open(country_threshold_table_path, 'w')
        country_threshold_table_file.write('country,percentile at 90% max,pixel count\n')

        percentile_per_country_file = open('%s_percentile.csv' % raster_id, 'w')
        percentile_per_country_file.write('country name,' + ','.join([str(x) for x in PERCENTILE_LIST]) + '\n')

        for world_border_feature in world_borders_layer:
            country_name = world_border_feature.GetField('nev_name')
            LOGGER.debug(country_name)
            country_workspace = os.path.join(COUNTRY_WORKSPACES, country_name)
            try:
                os.makedirs(country_workspace)
            except OSError:
                pass

            country_vector = os.path.join(
                country_workspace, '%s.gpkg' % country_name)
            country_vector_complete_token = os.path.join(
                country_workspace, '%s.COMPLETE' % country_name)
            extract_feature(
                world_borders_path, world_border_feature.GetFID(),
                wgs84_srs.ExportToWkt(), country_vector,
                country_vector_complete_token)

            country_raster_path = os.path.join(country_workspace, '%s_%s' % (
                country_name, os.path.basename(raster_path)))

            country_vector_info = pygeoprocessing.get_vector_info(country_vector)
            pygeoprocessing.warp_raster(
                raster_path, raster_info['pixel_size'], country_raster_path,
                'near', target_bb=country_vector_info['bounding_box'],
                vector_mask_options={'mask_vector_path': country_vector},
                working_dir=country_workspace)

            percentile_values = pygeoprocessing.raster_band_percentile(
                (country_raster_path, 1), country_workspace, PERCENTILE_LIST)
            percentile_per_country_file.write(
                '%s,' % country_name + ','.join(
                    [str(x) for x in percentile_values]) + '\n')
            percentile_per_country_file.flush()
            if len(percentile_values) != len(PERCENTILE_LIST):
                continue
            LOGGER.debug(
                "len percentile_values: %d len PERCENTILE_LIST: %d",
                len(percentile_values), len(PERCENTILE_LIST))

            cdf_array = [0.0] * len(percentile_values)

            nodata = pygeoprocessing.get_raster_info(
                country_raster_path)['nodata'][0]
            pixel_count = 0
            for _, data_block in pygeoprocessing.iterblocks(
                    (country_raster_path, 1)):
                nodata_mask = ~numpy.isclose(data_block, nodata)
                pixel_count += numpy.count_nonzero(nodata_mask)
                for index, percentile_value in enumerate(percentile_values):
                    cdf_array[index] += numpy.sum(data_block[
                        nodata_mask & (data_block >= percentile_value)])

            # threshold is at 90% says Becky
            threshold_limit = 0.9 * cdf_array[2]

            LOGGER.debug(cdf_array)
            fig, ax = matplotlib.pyplot.subplots()
            ax.plot(list(reversed(PERCENTILE_LIST)), cdf_array)
            f = scipy.interpolate.interp1d(
                cdf_array, list(reversed(PERCENTILE_LIST)))
            try:
                cdf_threshold = f(threshold_limit)
            except ValueError:
                LOGGER.exception(
                    "error when passing threshold_limit: %s\ncdf_array: %s" % (
                        threshold_limit, cdf_array))
                cdf_threshold = cdf_array[2]

            ax.plot([0, 100], [threshold_limit, threshold_limit], 'k:', linewidth=2)
            ax.plot([cdf_threshold, cdf_threshold], [cdf_array[0], cdf_array[-1]], 'k:', linewidth=2)

            ax.grid(True, linestyle='-.')
            ax.set_title(
                '%s CDF. 90%% max at %.2f and %.2f%%\nn=%d' % (country_name, threshold_limit, cdf_threshold, pixel_count))
            ax.set_ylabel('Sum of %s up to 100-percentile' % os.path.basename(raster_path))
            ax.set_ylabel('100-percentile')
            ax.tick_params(labelcolor='r', labelsize='medium', width=3)
            matplotlib.pyplot.autoscale(enable=True, tight=True)
            matplotlib.pyplot.savefig(
                os.path.join(COUNTRY_WORKSPACES, '%s_%s_cdf.png' % (country_name, raster_id)))
            country_threshold_table_file.write(
                '%s, %f, %d\n' % (country_name, cdf_threshold, pixel_count))
            country_threshold_table_file.flush()
        country_threshold_table_file.close()
    percentile_per_country_file.close()


def extract_feature(
        vector_path, feature_id, projection_wkt, target_vector_path,
        target_complete_token_path):
    """Make a local projection of a single feature in a vector.

    Parameters:
        vector_path (str): base vector in WGS84 coordinates.
        feature_id (int): FID for the feature to extract.
        projection_wkt (str): projection wkt code to project feature to.
        target_gpkg_vector_path (str): path to new GPKG vector that will
            contain only that feature.
        target_complete_token_path (str): path to a file that is created if
             the function successfully completes.

    Returns:
        None.

    """
    base_vector = gdal.OpenEx(vector_path, gdal.OF_VECTOR)
    base_layer = base_vector.GetLayer()
    feature = base_layer.GetFeature(feature_id)
    geom = feature.GetGeometryRef()

    epsg_srs = osr.SpatialReference()
    epsg_srs.ImportFromWkt(projection_wkt)

    base_srs = base_layer.GetSpatialRef()
    base_to_utm = osr.CoordinateTransformation(base_srs, epsg_srs)

    # clip out watershed to its own file
    # create a new shapefile
    if os.path.exists(target_vector_path):
        os.remove(target_vector_path)
    driver = ogr.GetDriverByName('GPKG')
    target_vector = driver.CreateDataSource(
        target_vector_path)
    target_layer = target_vector.CreateLayer(
        os.path.splitext(os.path.basename(target_vector_path))[0],
        epsg_srs, ogr.wkbMultiPolygon)
    layer_defn = target_layer.GetLayerDefn()
    feature_geometry = geom.Clone()
    base_feature = ogr.Feature(layer_defn)
    feature_geometry.Transform(base_to_utm)
    base_feature.SetGeometry(feature_geometry)
    target_layer.CreateFeature(base_feature)
    target_layer.SyncToDisk()
    geom = None
    feature_geometry = None
    base_feature = None
    target_layer = None
    target_vector = None
    base_layer = None
    base_vector = None
    with open(target_complete_token_path, 'w') as token_file:
        token_file.write(str(datetime.datetime.now()))

if __name__ == '__main__':
    main()