"""Calculate potential pollination."""
import logging
import os
import sys

from ecoshard import geoprocessing
from ecoshard import taskgraph
from osgeo import gdal
from osgeo import osr
import numpy
import raster_calculations_core
import scipy

WORKSPACE_DIR = 'workspace_potential_pollination'
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')

HAB_MASK_URL_MAP = {
    'nathab': 'https://storage.googleapis.com/ecoshard-root/working-shards/masked_nathab_esa_md5_40577bae3ef60519b1043bb8582a07af.tif',
    }
THRESHOLD_VAL = 0.3
TARGET_NODATA = -1.0

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


def main():
    """Entry point."""
    task_graph = taskgraph.TaskGraph(CHURN_DIR, -1, 5.0)
    try:
        os.makedirs(CHURN_DIR)
    except OSError:
        pass
    kernel_raster_path = os.path.join(CHURN_DIR, 'radial_kernel.tif')
    kernel_task = task_graph.add_task(
        func=create_radial_convolution_mask,
        args=(0.00277778, 2000., kernel_raster_path),
        target_path_list=[kernel_raster_path],
        task_name='make convolution kernel')
    for prefix_name, hab_mask_url in HAB_MASK_URL_MAP.items():
        hab_mask_path = os.path.join(
            CHURN_DIR, os.path.basename(hab_mask_url))
        fetch_hab_mask_task = task_graph.add_task(
            func=raster_calculations_core.download_url,
            args=(hab_mask_url, hab_mask_path),
            target_path_list=[hab_mask_path],
            task_name='fetch hab mask')

        natural_hab_proportion_raster_path = os.path.join(
            WORKSPACE_DIR, '%s_proportion.tif' % prefix_name)

        nathab_proportion_task = task_graph.add_task(
            func=geoprocessing.convolve_2d,
            args=[
                (hab_mask_path, 1), (kernel_raster_path, 1),
                natural_hab_proportion_raster_path],
            kwargs={
                'working_dir': CHURN_DIR,
                'ignore_nodata': True,
                'gtiff_creation_options': (
                    'TILED=YES', 'BIGTIFF=YES', 'COMPRESS=DEFLATE',
                    'PREDICTOR=3', 'BLOCKXSIZE=256', 'BLOCKYSIZE=256',
                    'NUM_THREADS=2'),
                'n_threads': 4},
            dependent_task_list=[fetch_hab_mask_task, kernel_task],
            target_path_list=[natural_hab_proportion_raster_path],
            task_name=(
                'calculate natural hab proportion'
                f' {os.path.basename(natural_hab_proportion_raster_path)}'))

        nathab_proportion_task.join()
        nathab_proportion_nodata = geoprocessing.get_raster_info(
            natural_hab_proportion_raster_path)['nodata'][0]

        potential_pollination_raster_path = os.path.join(
            WORKSPACE_DIR,
            '%s_potential_pollination.tif' % prefix_name)
        threshold_proportion_task = task_graph.add_task(
            func=geoprocessing.raster_calculator,
            args=(
                [(natural_hab_proportion_raster_path, 1),
                 (nathab_proportion_nodata, 'raw'),
                 (THRESHOLD_VAL, 'raw'), (TARGET_NODATA, 'raw')],
                interpolate_from_threshold,
                potential_pollination_raster_path, gdal.GDT_Float32,
                TARGET_NODATA),
            target_path_list=[potential_pollination_raster_path],
            dependent_task_list=[nathab_proportion_task],
            task_name='calculate potential pollination value')

    task_graph.join()
    task_graph.close()


def interpolate_from_threshold(
        base_array, base_array_nodata, threshold_val, target_nodata):
    """Interpolates values in base between 0..threshold..1."""
    result = numpy.empty(base_array.shape, dtype=numpy.float32)
    result[:] = target_nodata
    valid_mask = ~numpy.isclose(base_array, base_array_nodata)
    result[valid_mask] = numpy.interp(
        base_array[valid_mask], [0, THRESHOLD_VAL], [0, 1])
    return result


def create_radial_convolution_mask(
        pixel_size_degree, radius_meters, kernel_filepath):
    """Create a radial mask to sample pixels in convolution filter.

    Parameters:
        pixel_size_degree (float): size of pixel in degrees.
        radius_meters (float): desired size of radial mask in meters.

    Returns:
        A 2D numpy array that can be used in a convolution to aggregate a
        raster while accounting for partial coverage of the circle on the
        edges of the pixel.

    """
    degree_len_0 = 110574  # length at 0 degrees
    degree_len_60 = 111412  # length at 60 degrees
    pixel_size_m = pixel_size_degree * (degree_len_0 + degree_len_60) / 2.0
    pixel_radius = numpy.ceil(radius_meters / pixel_size_m)
    n_pixels = (int(pixel_radius) * 2 + 1)
    sample_pixels = 200
    mask = numpy.ones((sample_pixels * n_pixels, sample_pixels * n_pixels))
    mask[mask.shape[0]//2, mask.shape[0]//2] = 0
    distance_transform = scipy.ndimage.morphology.distance_transform_edt(mask)
    mask = None
    stratified_distance = distance_transform * pixel_size_m / sample_pixels
    distance_transform = None
    in_circle = numpy.where(stratified_distance <= 2000.0, 1.0, 0.0)
    stratified_distance = None
    reshaped = in_circle.reshape(
        in_circle.shape[0] // sample_pixels, sample_pixels,
        in_circle.shape[1] // sample_pixels, sample_pixels)
    kernel_array = numpy.sum(reshaped, axis=(1, 3)) / sample_pixels**2
    normalized_kernel_array = kernel_array / numpy.sum(kernel_array)
    reshaped = None

    driver = gdal.GetDriverByName('GTiff')
    kernel_raster = driver.Create(
        kernel_filepath.encode('utf-8'), n_pixels, n_pixels, 1,
        gdal.GDT_Float32, options=[
            'BIGTIFF=IF_SAFER', 'TILED=YES', 'BLOCKXSIZE=256',
            'BLOCKYSIZE=256'])

    # Make some kind of geotransform, it doesn't matter what but
    # will make GIS libraries behave better if it's all defined
    kernel_raster.SetGeoTransform([-180, 1, 0, 90, 0, -1])
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    kernel_raster.SetProjection(srs.ExportToWkt())
    kernel_band = kernel_raster.GetRasterBand(1)
    kernel_band.SetNoDataValue(TARGET_NODATA)
    kernel_band.WriteArray(normalized_kernel_array)


if __name__ == '__main__':
    main()
