"""Tracer for NDR watershed processing."""
import glob
import logging
import multiprocessing
import os
import shutil
import subprocess
import sys
import threading
import urllib
import zipfile

from inspring.ndr_plus.ndr_plus import ndr_plus
from osgeo import gdal
from osgeo import osr
import ecoshard
import pandas
import pygeoprocessing
import taskgraph

gdal.SetCacheMax(2**27)

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(processName)s %(levelname)s '
        '%(name)s [%(funcName)s:%(lineno)d] %(message)s'),
    filename='log.out')
LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.INFO)
WORKSPACE_DIR = 'cbd_workspace'
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshards')

USE_AG_LOAD_ID = 999

# All links in this dict is an ecoshard that will be downloaded to
# ECOSHARD_DIR
ECOSHARD_PREFIX = 'https://storage.googleapis.com/'

WATERSHED_ID = 'hydrosheds_15arcseconds'

# Known properties of the DEM:
DEM_ID = 'global_dem_3s'
DEM_TILE_DIR = os.path.join(ECOSHARD_DIR, 'global_dem_3s')
DEM_VRT_PATH = os.path.join(DEM_TILE_DIR, 'global_dem_3s.vrt')

# Global properties of the simulation
RETENTION_LENGTH_M = 150
K_VAL = 1.0
TARGET_CELL_LENGTH_M = 300
FLOW_THRESHOLD = int(500**2*90 / TARGET_CELL_LENGTH_M**2)
ROUTING_ALGORITHM = 'D8'
TARGET_WGS84_LENGTH_DEG = 10/3600
AREA_DEG_THRESHOLD = 0.000016 * 10  # this is 10 times larger than hydrosheds 1 "pixel" watersheds

BIOPHYSICAL_TABLE_IDS = {
    'esa_aries_rs3': 'Value',
    'nci-ndr-biophysical_table_forestry_grazing': 'ID', }

# ADD NEW DATA HERE
ECOSHARDS = {
    DEM_ID: f'{ECOSHARD_PREFIX}ipbes-ndr-ecoshard-data/global_dem_3s_blake2b_0532bf0a1bedbe5a98d1dc449a33ef0c.zip',
    WATERSHED_ID: f'{ECOSHARD_PREFIX}ipbes-ndr-ecoshard-data/watersheds_globe_HydroSHEDS_15arcseconds_blake2b_14ac9c77d2076d51b0258fd94d9378d4.zip',
    # Biophysical table:
    'esa_aries_rs3': f'{ECOSHARD_PREFIX}nci-ecoshards/nci-NDR-biophysical_table_ESA_ARIES_RS3_md5_74d69f7e7dc829c52518f46a5a655fb8.csv',
    'nci-ndr-biophysical_table_forestry_grazing': f'{ECOSHARD_PREFIX}nci-ecoshards/nci-NDR-biophysical_table_forestry_grazing_md5_7524f2996fcc929ddc3aaccde249d59f.csv',
    # Precip:
    'worldclim_2015': f'{ECOSHARD_PREFIX}ipbes-ndr-ecoshard-data/worldclim_2015_md5_16356b3770460a390de7e761a27dbfa1.tif',
    'worldclim_ssp3': f'{ECOSHARD_PREFIX}ipbes-ndr-ecoshard-data/precip_scenarios/he60pr50_md5_829fbd47b8fefb064ae837cbe4d9f4be.tif',
    # LULCs:
    'esacci-lc-l4-lccs-map-300m-p1y-2015-v2.0.7': f'{ECOSHARD_PREFIX}ipbes-ndr-ecoshard-data/ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif',
    'pnv_esa_iis': f'{ECOSHARD_PREFIX}ipbes-ndr-ecoshard-data/ESACCI_PNV_iis_OA_ESAclasses_max_ESAresproj_md5_e6575db589abb52c683d44434d428d80.tif',
    'extensification_bmps_irrigated': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/extensification_bmps_irrigated_md5_7f5928ea3dcbcc55b0df1d47fbeec312.tif',
    'extensification_bmps_rainfed': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/extensification_bmps_rainfed_md5_5350b6acebbff75bb71f27830098989f.tif',
    'extensification_current_practices': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/extensification_current_practices_md5_cbe24876a57999e657b885cf58c4981a.tif',
    'extensification_intensified_irrigated': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/extensification_intensified_irrigated_md5_215fe051b6bc84d3e15a4d1661b6b936.tif',
    'extensification_intensified_rainfed': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/extensification_intensified_rainfed_md5_47050c834831a6bc4644060fffffb052.tif',
    'fixedarea_bmps_irrigated': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/fixedarea_bmps_irrigated_md5_857517cbef7f21cd50f963b4fc9e7191.tif',
    'fixedarea_bmps_rainfed': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/fixedarea_bmps_rainfed_md5_3b220e236c818a28bd3f2f5eddcc48b0.tif',
    'fixedarea_intensified_irrigated': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/fixedarea_intensified_irrigated_md5_4990faf720ac68f95004635e4a2c3c74.tif',
    'fixedarea_intensified_rainfed': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/fixedarea_intensified_rainfed_md5_98ac886076a35507c962263ee6733581.tif',
    'global_potential_vegetation': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/global_potential_vegetation_md5_61ee1f0ffe1b6eb6f2505845f333cf30.tif',
    # Fertilizer
    'ag_load_2015':f'{ECOSHARD_PREFIX}ipbes-ndr-ecoshard-data/ag_load_scenarios/2015_ag_load_md5_4d8ea3cba0f1720afd4a1f2377fb974e.tif',
    'ag_load_ssp3':f'{ECOSHARD_PREFIX}ipbes-ndr-ecoshard-data/ag_load_scenarios/ssp3_2050_ag_load_md5_9fab631dfdae22d12cd92bb1983f9ef1.tif',
    'intensificationnapp_allcrops_irrigated_max_model_and_observednapprevb_bmps': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/IntensificationNapp_allcrops_irrigated_max_Model_and_observedNappRevB_BMPs_md5_ddc000f7ce7c0773039977319bcfcf5d.tif',
    'intensificationnapp_allcrops_rainfed_max_model_and_observednapprevb_bmps': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/IntensificationNapp_allcrops_rainfed_max_Model_and_observedNappRevB_BMPs_md5_fa2684c632ec2d0e0afb455b41b5d2a6.tif',
    'extensificationnapp_allcrops_rainfedfootprint_gapfilled_observednapprevb': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/ExtensificationNapp_allcrops_rainfedfootprint_gapfilled_observedNappRevB_md5_1185e457751b672c67cc8c6bf7016d03.tif',
    'intensificationnapp_allcrops_irrigated_max_model_and_observednapprevb': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/IntensificationNapp_allcrops_irrigated_max_Model_and_observedNappRevB_md5_9331ed220772b21f4a2c81dd7a2d7e10.tif',
    'intensificationnapp_allcrops_rainfed_max_model_and_observednapprevb': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/IntensificationNapp_allcrops_rainfed_max_Model_and_observedNappRevB_md5_1df3d8463641ffc6b9321e73973f3444.tif',

}

# DEFINE SCENARIOS HERE SPECIFYING 'lulc_id', 'precip_id', 'fertilizer_id', and 'biophysical_table_id'
# name the key of the scenario something unique
SCENARIOS = {
    'esa2015_driverssp3': {
        'lulc_id': 'esacci-lc-l4-lccs-map-300m-p1y-2015-v2.0.7',
        'precip_id': 'worldclim_ssp3',
        'fertilizer_id': 'ag_load_ssp3',
        'biophysical_table_id': 'esa_aries_rs3',
    },
    'pnv_driverssp3': {
        'lulc_id': 'pnv_esa_iis',
        'precip_id': 'worldclim_ssp3',
        'fertilizer_id': 'ag_load_ssp3',
        'biophysical_table_id': 'esa_aries_rs3',
    },
}


def create_empty_wgs84_raster(cell_size, nodata, target_path):
    """Create an empty wgs84 raster to cover all the world."""
    n_cols = int(360 // cell_size)
    n_rows = int(180 // cell_size)
    gtiff_driver = gdal.GetDriverByName('GTIFF')
    target_raster = gtiff_driver.Create(
        target_path, n_cols, n_rows, 1, gdal.GDT_Float32,
        options=(
            'TILED=YES', 'BIGTIFF=YES', 'COMPRESS=LZW',
            'BLOCKXSIZE=256', 'BLOCKYSIZE=256'))

    target_band = target_raster.GetRasterBand(1)
    target_band.SetNoDataValue(nodata)
    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)
    target_raster.SetProjection(wgs84_srs.ExportToWkt())
    target_raster.SetGeoTransform(
        [-180, cell_size, 0.0, 90.0, 0.0, -cell_size])
    target_raster = None


def stitch_worker(
        stitch_export_raster_path, stitch_modified_load_raster_path,
        stitch_queue):
    """Take elements from stitch queue and stitch into target."""
    try:
        export_raster_list = []
        modified_load_raster_list = []
        workspace_list = []

        while True:
            payload = stitch_queue.get()
            if payload is not None:
                (export_raster_path, modified_load_raster_path,
                 workspace_dir) = payload

                export_raster_list.append((export_raster_path, 1))
                modified_load_raster_list.append((modified_load_raster_path, 1))
                workspace_list.append(workspace_dir)

            if len(workspace_list) < 100 and payload is not None:
                continue

            worker_list = []
            for target_stitch_raster_path, raster_list in [
                    (stitch_export_raster_path, export_raster_list),
                    (stitch_modified_load_raster_path,
                     modified_load_raster_list)]:
                export_worker = multiprocessing.Process(
                    target=pygeoprocessing.stitch_rasters,
                    args=(
                        raster_list,
                        ['near']*len(raster_list),
                        (target_stitch_raster_path, 1)),
                    kwargs={
                        'overlap_algorithm': 'add',
                        'area_weight_m2_to_wgs84': True})
                export_worker.start()
                worker_list.append(export_worker)
            for worker in worker_list:
                worker.join()
            for workspace_dir in workspace_list:
                shutil.rmtree(workspace_dir)
            export_raster_list = []
            modified_load_raster_list = []
            workspace_list = []
            if payload is None:
                break
    except:
        LOGGER.exception('something bad happened on ndr stitcher')
        raise


def ndr_plus_and_stitch(
        watershed_path, watershed_fid,
        target_cell_length_m,
        retention_length_m,
        k_val,
        flow_threshold,
        routing_algorithm,
        dem_path,
        lulc_path,
        precip_path,
        custom_load_path,
        eff_n_lucode_map,
        load_n_lucode_map,
        target_export_raster_path,
        target_modified_load_raster_path,
        workspace_dir,
        stitch_queue):
    """Invoke ``inspring.ndr_plus`` with stitch.

        Same parameter list as ``inspring.ndr_plus`` with additional args:

        stitch_queue (queue): places export, load, and workspace path here to
            stitch globally and delete the workspace when complete.

        Return:
            ``None``
    """
    try:
        ndr_plus(
            watershed_path, watershed_fid,
            target_cell_length_m,
            retention_length_m,
            k_val,
            flow_threshold,
            routing_algorithm,
            dem_path,
            lulc_path,
            precip_path,
            custom_load_path,
            eff_n_lucode_map,
            load_n_lucode_map,
            target_export_raster_path,
            target_modified_load_raster_path,
            workspace_dir)
        stitch_queue.put(
            (target_export_raster_path, target_modified_load_raster_path,
             workspace_dir))
    except:
        LOGGER.exception(
            f'this exception happened on {watershed_path} {watershed_fid} but skipping with no problem')


def load_biophysical_table(biophysical_table_path, lulc_field_id):
    """Dump the biophysical table to two dictionaries indexable by lulc.

    Args:
        biophysical_table_path (str): biophysical table that indexes lulc
            codes to 'eff_n' and 'load_n' values. These value can have
            the field 'use raster' in which case they will be replaced with
            a custom raster layer for the lulc code.

    Return:
        A tuple of:
        * eff_n_lucode_map: index lulc to nitrogen efficiency
        * load_n_lucode_map: index lulc to base n load
    """
    biophysical_table = pandas.read_csv(biophysical_table_path)
    # clean up biophysical table
    biophysical_table = biophysical_table.fillna(0)
    biophysical_table.loc[
        biophysical_table['load_n'] == 'use raster', 'load_n'] = (
            USE_AG_LOAD_ID)
    biophysical_table['load_n'] = biophysical_table['load_n'].apply(
        pandas.to_numeric)

    eff_n_lucode_map = dict(
            zip(biophysical_table[lulc_field_id], biophysical_table['eff_n']))
    load_n_lucode_map = dict(
        zip(biophysical_table[lulc_field_id], biophysical_table['load_n']))
    return eff_n_lucode_map, load_n_lucode_map


def unzip(zipfile_path, target_unzip_dir):
    """Unzip zip to target_dir."""
    LOGGER.info(f'unzip {zipfile_path} to {target_unzip_dir}')
    os.makedirs(target_unzip_dir, exist_ok=True)
    with zipfile.ZipFile(zipfile_path, 'r') as zip_ref:
        zip_ref.extractall(target_unzip_dir)


def unzip_and_build_dem_vrt(
        zipfile_path, target_unzip_dir, expected_tiles_zip_path,
        target_vrt_path):
    """Build VRT of given tiles.

    Args:
        zipfile_path (str): source zip file to extract.
        target_unzip_dir (str): desired directory in which to extract
            the zipfile.
        expected_tiles_zip_path (str): the expected directory to find the
            geotiff tiles after the zipfile has been extracted to
            ``target_unzip_dir``.
        target_vrt_path (str): path to desired VRT file of those files.

    Return:
        ``None``
    """
    unzip(zipfile_path, target_unzip_dir)
    LOGGER.info('build vrt')
    subprocess.run(
        f'gdalbuildvrt {target_vrt_path} {expected_tiles_zip_path}/*.tif',
        shell=True)
    LOGGER.info(f'all done building {target_vrt_path}')


def main():
    """Entry point."""
    os.makedirs(WORKSPACE_DIR, exist_ok=True)
    task_graph = taskgraph.TaskGraph(
        WORKSPACE_DIR, multiprocessing.cpu_count())
    os.makedirs(ECOSHARD_DIR, exist_ok=True)
    ecoshard_path_map = {}
    LOGGER.info('scheduling downloads')
    for ecoshard_id, ecoshard_url in ECOSHARDS.items():
        ecoshard_path = os.path.join(
            ECOSHARD_DIR, os.path.basename(ecoshard_url))
        LOGGER.debug(f'download {ecoshard_url}')
        LOGGER.debug(f'dlcode: {urllib.request.urlopen(ecoshard_url).getcode()}')
        download_task = task_graph.add_task(
            func=ecoshard.download_url,
            args=(ecoshard_url, ecoshard_path),
            target_path_list=[ecoshard_path])
        ecoshard_path_map[ecoshard_id] = ecoshard_path
    LOGGER.info('waiting for downloads to finish')
    task_graph.join()

    # global DEM that's used
    task_graph.add_task(
        func=unzip_and_build_dem_vrt,
        args=(
            ecoshard_path_map[DEM_ID], ECOSHARD_DIR, DEM_TILE_DIR,
            DEM_VRT_PATH),
        target_path_list=[DEM_VRT_PATH],
        task_name='build DEM vrt')

    watershed_dir = os.path.join(
        ECOSHARD_DIR, 'watersheds_globe_HydroSHEDS_15arcseconds')
    expected_watershed_path = os.path.join(
        watershed_dir, 'af_bas_15s_beta.shp')

    task_graph.add_task(
        func=unzip,
        args=(ecoshard_path_map[WATERSHED_ID], ECOSHARD_DIR),
        target_path_list=[expected_watershed_path],
        task_name='unzip watersheds')

    task_graph.join()

    manager = multiprocessing.Manager()
    stitch_worker_list = []
    stitch_queue_list = []
    target_raster_list = []
    for scenario_id, scenario_vars in SCENARIOS.items():
        eff_n_lucode_map, load_n_lucode_map = load_biophysical_table(
            ecoshard_path_map[scenario_vars['biophysical_table_id']],
            BIOPHYSICAL_TABLE_IDS[scenario_vars['biophysical_table_id']])

        stitch_queue = manager.Queue()
        stitch_queue_list.append(stitch_queue)
        target_export_raster_path = os.path.join(
            WORKSPACE_DIR, f'{scenario_id}_{TARGET_CELL_LENGTH_M:.1f}_{ROUTING_ALGORITHM}_export.tif')
        target_modified_load_raster_path = os.path.join(
            WORKSPACE_DIR, f'{scenario_id}_{TARGET_CELL_LENGTH_M:.1f}_{ROUTING_ALGORITHM}_modified_load.tif')

        create_empty_wgs84_raster(
            TARGET_WGS84_LENGTH_DEG, -1, target_export_raster_path)
        create_empty_wgs84_raster(
            TARGET_WGS84_LENGTH_DEG, -1, target_modified_load_raster_path)

        target_raster_list.extend(
            [target_export_raster_path, target_modified_load_raster_path])

        stitch_worker_thread = threading.Thread(
            target=stitch_worker,
            args=(
                target_export_raster_path, target_modified_load_raster_path,
                stitch_queue))
        stitch_worker_thread.start()
        stitch_worker_list.append(stitch_worker_thread)

        for watershed_path in glob.glob(os.path.join(watershed_dir, '*.shp')):
            watershed_vector = gdal.OpenEx(watershed_path, gdal.OF_VECTOR)
            watershed_layer = watershed_vector.GetLayer()
            watershed_basename = os.path.splitext(os.path.basename(watershed_path))[0]
            for watershed_feature in watershed_layer:

                if watershed_feature.GetGeometryRef().Area() < AREA_DEG_THRESHOLD:
                    continue

                watershed_fid = watershed_feature.GetFID()
                local_workspace_dir = os.path.join(
                    WORKSPACE_DIR, scenario_id,
                    f'{watershed_basename}_{watershed_fid}')
                local_export_raster_path = os.path.join(
                    local_workspace_dir, os.path.basename(target_export_raster_path))
                local_modified_load_raster_path = os.path.join(
                    local_workspace_dir, os.path.basename(target_modified_load_raster_path))
                task_graph.add_task(
                    func=ndr_plus_and_stitch,
                    args=(
                        watershed_path, watershed_fid,
                        TARGET_CELL_LENGTH_M,
                        RETENTION_LENGTH_M,
                        K_VAL,
                        FLOW_THRESHOLD,
                        ROUTING_ALGORITHM,
                        DEM_VRT_PATH,
                        ecoshard_path_map[scenario_vars['lulc_id']],
                        ecoshard_path_map[scenario_vars['precip_id']],
                        ecoshard_path_map[scenario_vars['fertilizer_id']],
                        eff_n_lucode_map,
                        load_n_lucode_map,
                        local_export_raster_path,
                        local_modified_load_raster_path,
                        local_workspace_dir,
                        stitch_queue),
                    task_name=f'{watershed_basename}_{watershed_fid}')

    task_graph.join()
    task_graph.close()
    for stitch_queue in stitch_queue_list:
        stitch_queue.put(None)
    for stitch_worker_thread in stitch_worker_list:
        stitch_worker_thread.join()

    # TODO: build overviews and compress
    build_overview_list = []
    for target_raster in target_raster_list:
        compress_raster_path = os.path.join(
            WORKSPACE_DIR,
            f'compress_overview_{os.path.basename(target_raster)}')
        build_overview_process = multiprocessing.Process(
            target=compress_and_overview,
            args=(target_raster, compress_raster_path))
        build_overview_process.start()
        build_overview_list.append(build_overview_process)
    for process in build_overview_list:
        process.join()


def compress_and_overview(base_raster_path, target_raster_path):
    """Compress and overview base to raster."""
    ecoshard.compress_raster(base_raster_path, target_raster_path)
    ecoshard.build_overviews(target_raster_path)


if __name__ == '__main__':
    main()
