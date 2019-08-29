"""Remap landcover codes based on distance from stream."""
import datetime
import os
import sys
import logging
import zipfile

from osgeo import gdal
from osgeo import osr
import pygeoprocessing
import pygeoprocessing.routing
import pygeoprocessing.symbolic
import numpy
import taskgraph
import ecoshard

LOGGER = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.DEBUG,
    format=('%(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)

DEM_ECOSHARD_URL = 'https://storage.googleapis.com/critical-natural-capital-ecoshards/Dem10cr1_md5_1ec5d8b327316c8adc888dde96595a82.zip'
LULC_ECOSHARD_URL = 'https://storage.googleapis.com/critical-natural-capital-ecoshards/Base_LULC_CR_updated1_md5_a63f1e8a0538e268c6ae8701ccf0291b.tif'
STREAM_LAYER_ECOSHARD_URL = 'https://storage.googleapis.com/critical-natural-capital-ecoshards/Rivers_lascruces_KEL-20190827T205323Z-001_md5_76455ad11ee32423388f0bbf22f07795.zip'
WORKSPACE_DIR = 'raster_stream_buffer_workspace'


def conditional_convert_op(
        base_lulc, lulc_nodata, converted_lulc, buffer_1_array,
        buffer_5_array, high_slope_array, high_slope_nodata,
        stream_array, target_nodata):
    """Convert LULC to the converted one on the special cases.

    convert lulc to converted if:
        buffer_size_path_map[1] == 1
        or buffer_size_path_map[5] == 1 & steep_slope_mask_path

    """
    result = numpy.empty(base_lulc.shape, dtype=numpy.int16)
    result[:] = target_nodata
    lulc_nodata_mask = (base_lulc == lulc_nodata)
    result[~lulc_nodata_mask] = base_lulc[~lulc_nodata_mask]
    nodata_mask = (
        lulc_nodata_mask |
        (numpy.isclose(high_slope_array, high_slope_nodata)))
    valid_mask = (~nodata_mask) & (
        (buffer_1_array >= 1) | (
            (buffer_5_array >= 1) & (high_slope_array > 0)))
    result[valid_mask] = converted_lulc[valid_mask]
    stream_mask = (stream_array == 1) & (~nodata_mask)
    result[stream_mask] = 4
    return result


def mask_by_value_op(array, value, nodata):
    """Return 1 where array==value 0 otherwise."""
    result = numpy.empty_like(array)
    result[:] = 0
    result[array == value] = 1
    result[numpy.isclose(array, nodata)] = 2
    return result


def mask_by_inv_value_op(array, value, nodata):
    """Return 0 where array==value 1 otherwise."""
    result = numpy.empty_like(array)
    result[:] = 0
    result[array != value] = 1
    result[numpy.isclose(array, nodata)] = 2
    return result


def download_and_unzip(base_url, target_dir, done_token_path):
    """Download and unzip base_url to target_dir and write done token path."""
    path_to_zip_file = os.path.join(target_dir, os.path.basename(base_url))
    ecoshard.download_url(
        base_url, path_to_zip_file, skip_if_target_exists=False)
    zip_ref = zipfile.ZipFile(path_to_zip_file, 'r')
    zip_ref.extractall(target_dir)
    zip_ref.close()
    with open(done_token_path, 'w') as done_file:
        done_file.write(str(datetime.datetime.now()))


def burn_dem(
        dem_raster_path, streams_raster_path, target_burned_dem_path,
        burn_depth=10):
    """Burn streams into dem."""
    dem_raster_info = pygeoprocessing.get_raster_info(dem_raster_path)
    dem_nodata = dem_raster_info['nodata'][0]
    pygeoprocessing.new_raster_from_base(
        dem_raster_path, target_burned_dem_path, dem_raster_info['datatype'],
        [dem_nodata])

    burned_dem_raster = gdal.OpenEx(
        target_burned_dem_path, gdal.OF_RASTER | gdal.OF_UPDATE)
    burned_dem_band = burned_dem_raster.GetRasterBand(1)
    stream_raster = gdal.OpenEx(streams_raster_path, gdal.OF_RASTER)
    stream_band = stream_raster.GetRasterBand(1)
    for offset_dict, dem_block in pygeoprocessing.iterblocks(
            (dem_raster_path, 1)):
        stream_block = stream_band.ReadAsArray(**offset_dict)
        stream_mask = (
            (stream_block == 1) & ~numpy.isclose(dem_block, dem_nodata))
        filled_block = numpy.copy(dem_block)
        filled_block[stream_mask] = filled_block[stream_mask]-burn_depth
        burned_dem_band.WriteArray(
            filled_block, xoff=offset_dict['xoff'], yoff=offset_dict['yoff'])
    stream_band = None
    stream_raster = None
    burned_dem_band = None
    burned_dem_raster = None


def length_of_degree(lat):
    """Calculate the length of a degree in meters."""
    m1 = 111132.92
    m2 = -559.82
    m3 = 1.175
    m4 = -0.0023
    p1 = 111412.84
    p2 = -93.5
    p3 = 0.118
    lat_rad = lat * numpy.pi / 180
    latlen = (
        m1 + m2 * numpy.cos(2 * lat_rad) + m3 * numpy.cos(4 * lat_rad) +
        m4 * numpy.cos(6 * lat_rad))
    longlen = abs(
        p1 * numpy.cos(lat_rad) + p2 * numpy.cos(3 * lat_rad) + p3 *
        numpy.cos(5 * lat_rad))
    return max(latlen, longlen)


def rasterize_streams(
        base_raster_path, stream_vector_path, target_streams_raster_path):
    """Rasterize streams."""
    pygeoprocessing.new_raster_from_base(
        base_raster_path, target_streams_raster_path, gdal.GDT_Byte, [2],
        fill_value_list=[2])
    LOGGER.debug(stream_vector_path)
    pygeoprocessing.rasterize(
        stream_vector_path, target_streams_raster_path,
        burn_values=[1])


def hat_distance_kernel(pixel_radius, kernel_filepath):
    """Create a raster-based 0, 1 kernel path.

    Parameters:
        pixel_radius (int): Radius of the kernel in pixels.
        kernel_filepath (string): The path to the file on disk where this
            kernel should be stored.  If this file exists, it will be
            overwritten.

    Returns:
        None

    """
    kernel_size = int((pixel_radius)*2+1)
    driver = gdal.GetDriverByName('GTiff')
    kernel_dataset = driver.Create(
        kernel_filepath.encode('utf-8'), kernel_size, kernel_size, 1,
        gdal.GDT_Float32, options=[
            'BIGTIFF=IF_SAFER', 'TILED=YES', 'BLOCKXSIZE=256',
            'BLOCKYSIZE=256'])
    # Make some kind of geotransform, it doesn't matter what but
    # will make GIS libraries behave better if it's all defined
    kernel_dataset.SetGeoTransform([0, 1, 0, 0, 0, -1])
    srs = osr.SpatialReference()
    srs.SetWellKnownGeogCS('WGS84')
    kernel_dataset.SetProjection(srs.ExportToWkt())
    kernel_band = kernel_dataset.GetRasterBand(1)
    kernel_band.SetNoDataValue(-9999)
    cols_per_block, rows_per_block = kernel_band.GetBlockSize()
    row_indices, col_indices = numpy.indices(
        (kernel_size, kernel_size), dtype=numpy.float) - pixel_radius

    kernel_index_distances = numpy.hypot(row_indices, col_indices)
    kernel = kernel_index_distances <= pixel_radius
    kernel_band.WriteArray(kernel)

    kernel_band.FlushCache()
    kernel_dataset.FlushCache()
    kernel_band = None
    kernel_dataset = None


def linear_decay_kernel(pixel_radius, kernel_filepath):
    """Create a raster-based linear decay kernel path.

    Parameters:
        pixel_radius (int): Radius of the kernel in pixels.
        kernel_filepath (string): The path to the file on disk where this
            kernel should be stored.  If this file exists, it will be
            overwritten.

    Returns:
        None

    """
    kernel_size = int((pixel_radius)*2+1)
    driver = gdal.GetDriverByName('GTiff')
    kernel_dataset = driver.Create(
        kernel_filepath.encode('utf-8'), kernel_size, kernel_size, 1,
        gdal.GDT_Float32, options=[
            'BIGTIFF=IF_SAFER', 'TILED=YES', 'BLOCKXSIZE=256',
            'BLOCKYSIZE=256'])
    # Make some kind of geotransform, it doesn't matter what but
    # will make GIS libraries behave better if it's all defined
    kernel_dataset.SetGeoTransform([0, 1, 0, 0, 0, -1])
    srs = osr.SpatialReference()
    srs.SetWellKnownGeogCS('WGS84')
    kernel_dataset.SetProjection(srs.ExportToWkt())
    kernel_band = kernel_dataset.GetRasterBand(1)
    kernel_band.SetNoDataValue(-9999)
    cols_per_block, rows_per_block = kernel_band.GetBlockSize()
    row_indices, col_indices = numpy.indices(
        (kernel_size, kernel_size), dtype=numpy.float) - pixel_radius

    kernel_index_distances = numpy.hypot(row_indices, col_indices)
    inverse_distances = (pixel_radius - kernel_index_distances) / pixel_radius
    inverse_distances[inverse_distances < 0] = 0
    kernel_band.WriteArray(inverse_distances)

    kernel_band.FlushCache()
    kernel_dataset.FlushCache()
    kernel_band = None
    kernel_dataset = None


if __name__ == '__main__':
    try:
        os.makedirs(WORKSPACE_DIR)
    except OSError:
        pass
    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, 4, 5)
    dem_download_token_path = os.path.join(
        WORKSPACE_DIR, 'dem_downloaded.TOKEN')
    dem_raster_path = os.path.join(WORKSPACE_DIR, 'Dem10cr1', 'Dem10cr1')
    _ = task_graph.add_task(
        func=download_and_unzip,
        args=(DEM_ECOSHARD_URL, WORKSPACE_DIR, dem_download_token_path),
        target_path_list=[dem_download_token_path],
        task_name='download dem')

    stream_vector_path = os.path.join(WORKSPACE_DIR, 'Rivers_lascruces_KEL')
    stream_download_token_path = os.path.join(
        WORKSPACE_DIR, 'stream_downloaded.TOKEN')
    _ = task_graph.add_task(
        func=download_and_unzip,
        args=(STREAM_LAYER_ECOSHARD_URL, WORKSPACE_DIR,
              stream_download_token_path),
        target_path_list=[stream_download_token_path],
        task_name='download stream')

    lulc_raster_path = os.path.join(
        WORKSPACE_DIR, os.path.basename(LULC_ECOSHARD_URL))
    _ = task_graph.add_task(
        func=ecoshard.download_url,
        args=(LULC_ECOSHARD_URL, lulc_raster_path),
        target_path_list=[lulc_raster_path],
        task_name='download lulc')

    task_graph.join()

    base_raster_path_list = [dem_raster_path, lulc_raster_path]
    dem_raster_info = pygeoprocessing.get_raster_info(dem_raster_path)
    lulc_raster_info = pygeoprocessing.get_raster_info(lulc_raster_path)
    LOGGER.debug(dem_raster_info)
    LOGGER.debug(lulc_raster_info)
    aligned_raster_path_list = [
        '%s/aligned_%s' % (os.path.dirname(path), os.path.basename(path))
        for path in base_raster_path_list]
    align_task = task_graph.add_task(
        func=pygeoprocessing.align_and_resize_raster_stack,
        args=(
            base_raster_path_list, aligned_raster_path_list,
            ['near', 'near'], dem_raster_info['pixel_size'],
            'intersection'),
        kwargs={'target_sr_wkt': lulc_raster_info['projection']},
        target_path_list=aligned_raster_path_list,
        task_name='align rasters')

    rasterized_streams_raster_path = os.path.join(
        WORKSPACE_DIR, 'rasterized_streams.tif')
    rasterize_streams_task = task_graph.add_task(
            func=rasterize_streams,
            args=(aligned_raster_path_list[0], stream_vector_path,
                  rasterized_streams_raster_path),
            target_path_list=[rasterized_streams_raster_path],
            dependent_task_list=[align_task],
            task_name='rasterize streams')

    burned_dem_path = os.path.join(WORKSPACE_DIR, 'burned_dem.tif')
    burn_dem_task = task_graph.add_task(
        func=burn_dem,
        args=(aligned_raster_path_list[0], rasterized_streams_raster_path,
              burned_dem_path),
        target_path_list=[burned_dem_path],
        dependent_task_list=[rasterize_streams_task],
        task_name='burn streams')

    filled_dem_raster_path = os.path.join(
        WORKSPACE_DIR, 'filled_dem.tif')
    fill_pits_task = task_graph.add_task(
        func=pygeoprocessing.routing.fill_pits,
        args=(
            (burned_dem_path, 1), filled_dem_raster_path),
        kwargs={'working_dir': WORKSPACE_DIR},
        dependent_task_list=[burn_dem_task],
        target_path_list=[filled_dem_raster_path],
        task_name='fill pits')

    slope_raster_path = os.path.join(WORKSPACE_DIR, 'slope.tif')
    slope_task = task_graph.add_task(
        func=pygeoprocessing.calculate_slope,
        args=((aligned_raster_path_list[0], 1), slope_raster_path),
        target_path_list=[slope_raster_path],
        dependent_task_list=[align_task],
        task_name='calculate slope')

    slope_avg_pixel_radius = 5
    slope_avg_kernel_filepath = os.path.join(
        WORKSPACE_DIR, 'slope_kernel_%d.tif' % slope_avg_pixel_radius)
    kernel_task = task_graph.add_task(
        func=linear_decay_kernel,
        args=(slope_avg_pixel_radius, slope_avg_kernel_filepath),
        target_path_list=[slope_avg_kernel_filepath],
        task_name='make slope average kernel %d' % slope_avg_pixel_radius)
    average_slope_raster = os.path.join(
        WORKSPACE_DIR, '%d_avg_slope.tif' % (slope_avg_pixel_radius))
    avg_slope_task = task_graph.add_task(
        func=pygeoprocessing.convolve_2d,
        args=(
            (slope_raster_path, 1), (slope_avg_kernel_filepath, 1),
            average_slope_raster),
        kwargs={
            'working_dir': WORKSPACE_DIR,
            'ignore_nodata': True,
            'normalize_kernel': True,
            },
        dependent_task_list=[kernel_task, slope_task],
        target_path_list=[average_slope_raster],
        task_name='slope average to %d pixels' % slope_avg_pixel_radius)

    flow_direction_path = os.path.join(WORKSPACE_DIR, 'mfd_flow_dir.tif')
    flow_dir_task = task_graph.add_task(
        func=pygeoprocessing.routing.flow_dir_mfd,
        args=((filled_dem_raster_path, 1), flow_direction_path),
        kwargs={'working_dir': WORKSPACE_DIR},
        target_path_list=[flow_direction_path],
        dependent_task_list=[fill_pits_task],
        task_name='flow dir')

    flow_accum_path = os.path.join(WORKSPACE_DIR, 'flow_accum.tif')
    flow_accum_task = task_graph.add_task(
        func=pygeoprocessing.routing.flow_accumulation_mfd,
        args=((flow_direction_path, 1), flow_accum_path),
        target_path_list=[flow_accum_path],
        dependent_task_list=[flow_dir_task],
        task_name='flow accum')

    stream_raster_path = os.path.join(WORKSPACE_DIR, 'stream.tif')
    stream_task = task_graph.add_task(
        func=pygeoprocessing.routing.extract_streams_mfd,
        args=((flow_accum_path, 1), (flow_direction_path, 1), 1000,
              stream_raster_path),
        target_path_list=[stream_raster_path],
        dependent_task_list=[flow_accum_task],
        task_name='calc stream')

    buffer_size_path_map = {}
    for pixel_radius in [1, 5]:
        kernel_filepath = os.path.join(
            WORKSPACE_DIR, '%d_kernel.tif' % pixel_radius)
        kernel_task = task_graph.add_task(
            func=hat_distance_kernel,
            args=(pixel_radius, kernel_filepath),
            target_path_list=[kernel_filepath],
            task_name='make kernel')
        stream_buffer_raster = os.path.join(
            WORKSPACE_DIR, '%d_stream_buffer.tif' % (pixel_radius))
        buffer_size_path_map[pixel_radius] = stream_buffer_raster
        task_graph.add_task(
            func=pygeoprocessing.convolve_2d,
            args=(
                (stream_raster_path, 1), (kernel_filepath, 1),
                stream_buffer_raster),
            kwargs={
                'working_dir': WORKSPACE_DIR,
                'ignore_nodata': True,
                },
            dependent_task_list=[kernel_task, stream_task],
            target_path_list=[stream_buffer_raster],
            task_name='stream buffer %d pixels' % pixel_radius)
    slope_threshold = 40.0
    slope_mask_nodata = -9999
    steep_slope_mask_path = os.path.join(
        WORKSPACE_DIR, 'steep_slope_%.2f_mask.tif' % slope_threshold)
    task_graph.add_task(
        func=pygeoprocessing.symbolic.evaluate_raster_calculator_expression,
        args=(
            'slope > %f' % slope_threshold,
            {'slope': (average_slope_raster, 1)}, slope_mask_nodata,
            steep_slope_mask_path),
        target_path_list=[steep_slope_mask_path],
        dependent_task_list=[avg_slope_task],
        task_name='mask slope')

    lulc_to_converted_map = {
        0: 100,
        1: 1,
        2: 102,
        3: 103,
        4: 4,
        5: 5,
        6: 106,
        7: 7,
        8: 108,
        9: 109,
        10: 110,
        11: 111,
        12: 12,
        13: 113,
        14: 14,
        15: 15,
        16: 16,
        21: 21,
        22: 122,
        23: 123,
        24: 24,
    }

    target_lulc_nodata = -1
    potential_converted_landover_raster_path = os.path.join(
        WORKSPACE_DIR, 'potential_converted_lulc.tif')
    converted_lulc_task = task_graph.add_task(
        func=pygeoprocessing.reclassify_raster,
        args=(
            (aligned_raster_path_list[1], 1), lulc_to_converted_map,
            potential_converted_landover_raster_path, gdal.GDT_Int16,
            target_lulc_nodata),
        kwargs={'values_required': True},
        target_path_list=[potential_converted_landover_raster_path],
        dependent_task_list=[align_task],
        task_name='calculate converted')

    task_graph.join()

    converted_landover_raster_path = os.path.join(
        WORKSPACE_DIR, 'converted_lulc.tif')
    base_lulc_nodata = lulc_raster_info['nodata'][0]
    task_graph.add_task(
        func=pygeoprocessing.raster_calculator,
        args=(
            ((aligned_raster_path_list[1], 1), (base_lulc_nodata, 'raw'),
             (potential_converted_landover_raster_path, 1),
             (buffer_size_path_map[1], 1), (buffer_size_path_map[5], 1),
             (steep_slope_mask_path, 1), (slope_mask_nodata, 'raw'),
             (stream_raster_path, 1),
             (target_lulc_nodata, 'raw')),
            conditional_convert_op, converted_landover_raster_path,
            gdal.GDT_Int16, target_lulc_nodata),
        target_path_list=[converted_landover_raster_path],
        task_name='convert landcover')

    task_graph.join()
    task_graph.close()
