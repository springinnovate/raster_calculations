"""Map people fed equivalents back to ESA habitat."""
import os
import logging
import sys

import scipy
import numpy
from osgeo import osr
from osgeo import gdal
import pygeoprocessing
import taskgraph
import raster_calculations_core
import compress_and_overview

BASE_RASTER_URL_MAP = {
    'ppl_fed': 'https://storage.googleapis.com/ecoshard-root/working-shards/pollination_ppl_fed_on_ag_10s_esa_md5_0fb6bd172901703755b33dae2c9f1b92.tif',
    'hab_mask': 'https://storage.googleapis.com/ecoshard-root/working-shards/masked_nathab_esa_md5_40577bae3ef60519b1043bb8582a07af.tif',
}


WORKSPACE_DIR = 'workspace_realized_pollination'
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshard')
REALIZED_POLLINATION_RASTER_PATH = os.path.join(
    WORKSPACE_DIR, 'realized_pollination.tif')
REALIZED_POLLINATION_COMPRESSED_RASTER_PATH = os.path.join(
    WORKSPACE_DIR, 'realized_pollination_with_overviews.tif')
TARGET_NODATA = -1

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


def _nodata_to_zero_op(base_array, base_nodata):
    """Convert nodata to zero."""
    result = numpy.copy(base_array)
    result[numpy.isclose(base_array, base_nodata)] = 0.0
    return result


def main():
    """Entry point."""
    for dir_path in [WORKSPACE_DIR, CHURN_DIR, ECOSHARD_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass
    task_graph = taskgraph.TaskGraph(CHURN_DIR, -1, 5.0)
    kernel_raster_path = os.path.join(CHURN_DIR, 'radial_kernel.tif')
    kernel_task = task_graph.add_task(
        func=create_flat_radial_convolution_mask,
        args=(0.00277778, 2000., kernel_raster_path),
        target_path_list=[kernel_raster_path],
        task_name='make convolution kernel')
    hab_fetch_path_map = {}
    # download hab mask and ppl fed equivalent raster
    for raster_id, raster_url in BASE_RASTER_URL_MAP.items():
        raster_path = os.path.join(ECOSHARD_DIR, os.path.basename(raster_url))
        _ = task_graph.add_task(
            func=raster_calculations_core.download_url,
            args=(raster_url, raster_path),
            target_path_list=[raster_path],
            task_name='fetch hab mask')
        hab_fetch_path_map[raster_id] = raster_path
    task_graph.join()

    hab_mask_raster_info = pygeoprocessing.get_raster_info(
        hab_fetch_path_map['hab_mask'])

    ppl_fed_raster_info = pygeoprocessing.get_raster_info(
        hab_fetch_path_map['ppl_fed'])

    ppl_fed_nodata_to_zero_path = os.path.join(
        CHURN_DIR, 'ppl_fed__nodata_to_zero.tif')

    task_graph.add_task(
        func=pygeoprocessing.raster_calculator,
        args=(
            [(hab_fetch_path_map['ppl_fed'], 1),
             (ppl_fed_raster_info['nodata'][0], 'raw')],
            _nodata_to_zero_op, ppl_fed_nodata_to_zero_path,
            gdal.GDT_Float32, None),
        target_path_list=[ppl_fed_nodata_to_zero_path],
        task_name='hab mask nodata to zero')
    task_graph.join()

    # calculate extent of ppl fed by 2km.
    ppl_fed_reach_raster_path = os.path.join(CHURN_DIR, 'ppl_fed_reach.tif')
    ppl_fed_reach_task = task_graph.add_task(
        func=pygeoprocessing.convolve_2d,
        args=[
            (ppl_fed_nodata_to_zero_path, 1), (kernel_raster_path, 1),
            ppl_fed_reach_raster_path],
        kwargs={
            'working_dir': CHURN_DIR,
            'mask_nodata': False,
            'raster_driver_creation_tuple': (
                'GTiff', (
                    'TILED=YES', 'BIGTIFF=YES', 'COMPRESS=ZSTD',
                    'PREDICTOR=1', 'BLOCKXSIZE=256', 'BLOCKYSIZE=256',
                    'NUM_THREADS=2')),
                'n_threads': 4},
        dependent_task_list=[kernel_task],
        target_path_list=[ppl_fed_reach_raster_path],
        task_name=(
            'calculate natural hab proportion'
            f' {os.path.basename(ppl_fed_reach_raster_path)}'))

    # mask ppl fed reach by the hab mask.
    raster_calculations_core.evaluate_calculation(
        {
            'expression': 'ppl_fed_reach*(hab_mask>0.0)',
            'symbol_to_path_map': {
                'ppl_fed_reach': ppl_fed_reach_raster_path,
                'hab_mask': hab_fetch_path_map['hab_mask'],
            },
            'target_pixel_size': hab_mask_raster_info['pixel_size'],
            'target_nodata': TARGET_NODATA,
            'target_raster_path': REALIZED_POLLINATION_RASTER_PATH,
        }, task_graph, CHURN_DIR)
    task_graph.join()

    compress_and_overview.compress_to(
        task_graph, REALIZED_POLLINATION_RASTER_PATH, 'bilinear',
        REALIZED_POLLINATION_COMPRESSED_RASTER_PATH)

    task_graph.close()


def create_flat_radial_convolution_mask(
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
    normalized_kernel_array = kernel_array / numpy.max(kernel_array)
    LOGGER.debug(normalized_kernel_array)
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
