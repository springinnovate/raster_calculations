import collections
import os
import logging
import sys
import subprocess
import shutil
import tempfile

from ecoshard import taskgraph
from ecoshard import geoprocessing
from osgeo import gdal
import numpy

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
logging.getLogger('ecoshard.taskgraph').setLevel(logging.INFO)
LOGGER = logging.getLogger(__name__)


def make_top_nth_percentile_masks(
        base_raster_path, top_percentile_list, target_raster_path_pattern):
    """Mask base by mask such that any nodata in mask is set to nodata in base."""
    ordered_top_percentile_list = list(sorted(top_percentile_list, reverse=True))
    # need to convert this to "gte" format so if top 10th percent, we get 90th percentile
    raw_percentile_list = [100-float(x) for x in ordered_top_percentile_list]
    working_dir = tempfile.mkdtemp(
        prefix='percentile_sort_', dir=os.path.dirname(target_raster_path_pattern))
    os.makedirs(working_dir, exist_ok=True)
    percentile_values = geoprocessing.raster_band_percentile(
        (base_raster_path, 1), working_dir,
        raw_percentile_list,
        heap_buffer_size=2**28,
        ffi_buffer_size=2**10)
    base_info = geoprocessing.get_raster_info(base_raster_path)
    base_nodata = base_info['nodata'][0]

    target_raster_path_result_list = []
    for percentile_value, top_nth_percentile in zip(percentile_values, ordered_top_percentile_list):
        def mask_nth_percentile_op(base_array):
            result = numpy.zeros(base_array.shape)
            valid_mask = (base_array != base_nodata) & numpy.isfinite(base_array)
            valid_mask &= (base_array >= percentile_value)
            result[valid_mask] = 1
            return result

        target_raster_path = target_raster_path_pattern.format(percentile=top_nth_percentile)
        target_raster_path_result_list.append(target_raster_path)
        pre_cog_target_raster_path = os.path.join(working_dir, os.path.basename(target_raster_path))
        geoprocessing.single_thread_raster_calculator(
            [(base_raster_path, 1)], mask_nth_percentile_op,
            pre_cog_target_raster_path, gdal.GDT_Byte, None)
        subprocess.check_call(
            f'gdal_translate {pre_cog_target_raster_path} {target_raster_path} -of COG -co BIGTIFF=YES')
    shutil.rmtree(working_dir)
    return target_raster_path_result_list


def raster_op(op_str, raster_path_a, raster_path_b, target_raster_path, target_nodata=None, target_datatype=None):
    base_raster_list = [
        raster_path_a,
        raster_path_b]
    working_dir = tempfile.mkdtemp(
        prefix='ok_to_delete_', dir=os.path.dirname(target_raster_path))
    target_basename = os.path.splitext(os.path.basename(target_raster_path))[0]
    aligned_target_raster_path_list = [
        os.path.join(working_dir, f'align_{target_basename}_{os.path.basename(path)}')
        for path in base_raster_list]
    pixel_size = geoprocessing.get_raster_info(
        raster_path_a)['pixel_size']
    geoprocessing.align_and_resize_raster_stack(
        base_raster_list, aligned_target_raster_path_list, ['near']*2,
        pixel_size, 'intersection')

    raster_info = geoprocessing.get_raster_info(raster_path_a)

    a_nodata = geoprocessing.get_raster_info(aligned_target_raster_path_list[0])['nodata'][0]
    b_nodata = geoprocessing.get_raster_info(aligned_target_raster_path_list[1])['nodata'][0]
    if target_nodata is None:
        target_nodata = a_nodata

    def _op(array_a, array_b):
        result = numpy.full(array_a.shape, target_nodata)
        valid_mask = numpy.ones(array_a.shape, dtype=bool)
        for array, nodata in [(array_a, a_nodata), (array_b, b_nodata)]:
            if nodata is not None:
                valid_mask &= array != nodata
            valid_mask &= numpy.isfinite(array)
        result[valid_mask] = eval(f'array_a[valid_mask]{op_str}array_b[valid_mask]')
        return result

    if target_datatype is None:
        target_datatype = raster_info['datatype']

    pre_cog_target_raster_path = os.path.join(
        working_dir, os.path.basename(target_raster_path))
    geoprocessing.single_thread_raster_calculator(
        [(path, 1) for path in aligned_target_raster_path_list],
        _op, pre_cog_target_raster_path, target_datatype, target_nodata)
    subprocess.check_call(
        f'gdal_translate {pre_cog_target_raster_path} {target_raster_path} -of COG -co BIGTIFF=YES')
    try:
        shutil.rmtree(working_dir)
    except PermissionError:
        LOGGER.exception(f'could not delete {working_dir}, but leaving it there to keep going')

def main():
    RESULTS_DIR = 'D:\\repositories\\wwf-sipa\\final_results'
    os.makedirs(RESULTS_DIR, exist_ok=True)

    # diff x benes x services (4) x scenarios (2) x climage (2)
    service_list = ['flood_mitigation', 'recharge', 'sediment']
    country_list = ['PH', 'IDN']
    scenario_list = ['restoration', 'conservation_inf']
    climate_list = ['ssp245', '']
    beneficiary_list = ['dspop', 'road']
    top_percentile_list = [25, 10]

    DIFF_FLOOD_MITIGATION_IDN_CONSERVATION_INF = os.path.join(RESULTS_DIR, "diff_flood_mitigation_IDN_conservation_inf.tif")
    DIFF_FLOOD_MITIGATION_IDN_CONSERVATION_INF_SSP245 = os.path.join(RESULTS_DIR, "diff_flood_mitigation_IDN_conservation_inf_ssp245.tif")
    DIFF_FLOOD_MITIGATION_IDN_RESTORATION = os.path.join(RESULTS_DIR, "diff_flood_mitigation_IDN_restoration.tif")
    DIFF_FLOOD_MITIGATION_IDN_RESTORATION_SSP245 = os.path.join(RESULTS_DIR, "diff_flood_mitigation_IDN_restoration_ssp245.tif")
    DIFF_FLOOD_MITIGATION_PH_CONSERVATION_INF = os.path.join(RESULTS_DIR, "diff_flood_mitigation_PH_conservation_inf.tif")
    DIFF_FLOOD_MITIGATION_PH_CONSERVATION_INF_SSP245 = os.path.join(RESULTS_DIR, "diff_flood_mitigation_PH_conservation_inf_ssp245.tif")
    DIFF_FLOOD_MITIGATION_PH_RESTORATION = os.path.join(RESULTS_DIR, "diff_flood_mitigation_PH_restoration.tif")
    DIFF_FLOOD_MITIGATION_PH_RESTORATION_SSP245 = os.path.join(RESULTS_DIR, "diff_flood_mitigation_PH_restoration_ssp245.tif")
    DIFF_QUICKFLOW_IDN_CONSERVATION_INF = os.path.join(RESULTS_DIR, 'diff_quickflow_IDN_conservation_inf.tif')
    DIFF_QUICKFLOW_IDN_CONSERVATION_INF_SSP245 = os.path.join(RESULTS_DIR, 'diff_quickflow_IDN_conservation_inf_ssp245.tif')
    DIFF_QUICKFLOW_IDN_RESTORATION = os.path.join(RESULTS_DIR, 'diff_quickflow_IDN_restoration.tif')
    DIFF_QUICKFLOW_IDN_RESTORATION_SSP245 = os.path.join(RESULTS_DIR, 'diff_quickflow_IDN_restoration_ssp245.tif')
    DIFF_QUICKFLOW_PH_CONSERVATION_INF = os.path.join(RESULTS_DIR, 'diff_quickflow_PH_conservation_inf.tif')
    DIFF_QUICKFLOW_PH_CONSERVATION_INF_SSP245 = os.path.join(RESULTS_DIR, 'diff_quickflow_PH_conservation_inf_ssp245.tif')
    DIFF_QUICKFLOW_PH_RESTORATION = os.path.join(RESULTS_DIR, 'diff_quickflow_PH_restoration.tif')
    DIFF_QUICKFLOW_PH_RESTORATION_SSP245 = os.path.join(RESULTS_DIR, 'diff_quickflow_PH_restoration_ssp245.tif')
    DIFF_RECHARGE_IDN_CONSERVATION_INF = os.path.join(RESULTS_DIR, 'diff_recharge_IDN_conservation_inf.tif')
    DIFF_RECHARGE_IDN_CONSERVATION_INF_SSP245 = os.path.join(RESULTS_DIR, "diff_recharge_IDN_conservation_inf_ssp245.tif")
    DIFF_RECHARGE_IDN_RESTORATION = os.path.join(RESULTS_DIR, 'diff_recharge_IDN_restoration.tif')
    DIFF_RECHARGE_IDN_RESTORATION_SSP245 = os.path.join(RESULTS_DIR, "diff_recharge_IDN_restoration_ssp245.tif")
    DIFF_RECHARGE_PH_CONSERVATION_INF = os.path.join(RESULTS_DIR, 'diff_recharge_PH_conservation_inf.tif')
    DIFF_RECHARGE_PH_CONSERVATION_INF_SSP245 = os.path.join(RESULTS_DIR, "diff_recharge_PH_conservation_inf_ssp245.tif")
    DIFF_RECHARGE_PH_RESTORATION = os.path.join(RESULTS_DIR, 'diff_recharge_PH_restoration.tif')
    DIFF_RECHARGE_PH_RESTORATION_SSP245 = os.path.join(RESULTS_DIR, "diff_recharge_PH_restoration_ssp245.tif")
    DIFF_SEDIMENT_IDN_CONSERVATION_INF = os.path.join(RESULTS_DIR, 'diff_sediment_IDN_conservation_inf.tif')
    DIFF_SEDIMENT_IDN_CONSERVATION_INF_SSP245 = os.path.join(RESULTS_DIR, "diff_sediment_IDN_conservation_inf_ssp245.tif")
    DIFF_SEDIMENT_IDN_RESTORATION = os.path.join(RESULTS_DIR, 'diff_sediment_IDN_restoration.tif')
    DIFF_SEDIMENT_IDN_RESTORATION_SSP245 = os.path.join(RESULTS_DIR, "diff_sediment_IDN_restoration_ssp245.tif")
    DIFF_SEDIMENT_PH_CONSERVATION_INF = os.path.join(RESULTS_DIR, 'diff_sediment_PH_conservation_inf.tif')
    DIFF_SEDIMENT_PH_CONSERVATION_INF_SSP245 = os.path.join(RESULTS_DIR, "diff_sediment_PH_conservation_inf_ssp245.tif")
    DIFF_SEDIMENT_PH_RESTORATION = os.path.join(RESULTS_DIR, 'diff_sediment_PH_restoration.tif')
    DIFF_SEDIMENT_PH_RESTORATION_SSP245 = os.path.join(RESULTS_DIR, "diff_sediment_PH_restoration_ssp245.tif")
    FLOOD_MITIGATION_IDN_BASELINE_HISTORICAL_CLIMATE = os.path.join(RESULTS_DIR, "flood_mitigation_IDN_baseline_historical_climate.tif")
    FLOOD_MITIGATION_PH_BASELINE_HISTORICAL_CLIMATE = os.path.join(RESULTS_DIR, "flood_mitigation_PH_baseline_historical_climate.tif")

    DSPOP_SERVICE_FLOOD_MITIGATION_IDN_CONSERVATION_INF = os.path.join(RESULTS_DIR, "service_dspop_flood_mitigation_IDN_conservation_inf.tif")
    DSPOP_SERVICE_FLOOD_MITIGATION_IDN_RESTORATION = os.path.join(RESULTS_DIR, "service_dspop_flood_mitigation_IDN_restoration.tif")
    DSPOP_SERVICE_FLOOD_MITIGATION_PH_CONSERVATION_INF = os.path.join(RESULTS_DIR, "service_dspop_flood_mitigation_PH_conservation_inf.tif")
    DSPOP_SERVICE_FLOOD_MITIGATION_PH_RESTORATION = os.path.join(RESULTS_DIR, "service_dspop_flood_mitigation_PH_restoration.tif")
    DSPOP_SERVICE_RECHARGE_IDN_CONSERVATION_INF = os.path.join(RESULTS_DIR, "service_dspop_recharge_IDN_conservation_inf.tif")
    ROAD_SERVICE_FLOOD_MITIGATION_IDN_CONSERVATION_INF = os.path.join(RESULTS_DIR, "service_road_flood_mitigation_IDN_conservation_inf.tif")
    ROAD_SERVICE_FLOOD_MITIGATION_IDN_RESTORATION = os.path.join(RESULTS_DIR, "service_road_flood_mitigation_IDN_restoration.tif")
    ROAD_SERVICE_FLOOD_MITIGATION_PH_CONSERVATION_INF = os.path.join(RESULTS_DIR, "service_road_flood_mitigation_PH_conservation_inf.tif")
    ROAD_SERVICE_FLOOD_MITIGATION_PH_RESTORATION = os.path.join(RESULTS_DIR, "service_road_flood_mitigation_PH_restoration.tif")
    ROAD_SERVICE_RECHARGE_IDN_CONSERVATION_INF = os.path.join(RESULTS_DIR, "service_road_recharge_IDN_conservation_inf.tif")

    DSPOP_SERVICE_RECHARGE_IDN_RESTORATION = os.path.join(RESULTS_DIR, "service_dspop_recharge_IDN_restoration.tif")
    DSPOP_SERVICE_RECHARGE_PH_CONSERVATION_INF = os.path.join(RESULTS_DIR, "service_dspop_recharge_PH_conservation_inf.tif")
    DSPOP_SERVICE_RECHARGE_PH_RESTORATION = os.path.join(RESULTS_DIR, "service_dspop_recharge_PH_restoration.tif")
    DSPOP_SERVICE_SEDIMENT_IDN_CONSERVATION_INF = os.path.join(RESULTS_DIR, "service_dspop_sediment_IDN_conservation_inf.tif")
    DSPOP_SERVICE_SEDIMENT_IDN_RESTORATION = os.path.join(RESULTS_DIR, "service_dspop_sediment_IDN_restoration.tif")
    DSPOP_SERVICE_SEDIMENT_PH_CONSERVATION_INF = os.path.join(RESULTS_DIR, "service_dspop_sediment_PH_conservation_inf.tif")
    DSPOP_SERVICE_SEDIMENT_PH_RESTORATION = os.path.join(RESULTS_DIR, "service_dspop_sediment_PH_restoration.tif")
    ROAD_SERVICE_RECHARGE_IDN_RESTORATION = os.path.join(RESULTS_DIR, "service_road_recharge_IDN_restoration.tif")
    ROAD_SERVICE_RECHARGE_PH_CONSERVATION_INF = os.path.join(RESULTS_DIR, "service_road_recharge_PH_conservation_inf.tif")
    ROAD_SERVICE_RECHARGE_PH_RESTORATION = os.path.join(RESULTS_DIR, "service_road_recharge_PH_restoration.tif")
    ROAD_SERVICE_SEDIMENT_IDN_CONSERVATION_INF = os.path.join(RESULTS_DIR, "service_road_sediment_IDN_conservation_inf.tif")
    ROAD_SERVICE_SEDIMENT_IDN_RESTORATION = os.path.join(RESULTS_DIR, "service_road_sediment_IDN_restoration.tif")
    ROAD_SERVICE_SEDIMENT_PH_CONSERVATION_INF = os.path.join(RESULTS_DIR, "service_road_sediment_PH_conservation_inf.tif")
    ROAD_SERVICE_SEDIMENT_PH_RESTORATION = os.path.join(RESULTS_DIR, "service_road_sediment_PH_restoration.tif")

    DSPOP_SERVICE_FLOOD_MITIGATION_IDN_CONSERVATION_INF_SSP245 = os.path.join(RESULTS_DIR, "service_dspop_flood_mitigation_IDN_conservation_inf_ssp245.tif")
    DSPOP_SERVICE_FLOOD_MITIGATION_IDN_RESTORATION_SSP245 = os.path.join(RESULTS_DIR, "service_dspop_flood_mitigation_IDN_restoration_ssp245.tif")
    DSPOP_SERVICE_FLOOD_MITIGATION_PH_CONSERVATION_INF_SSP245 = os.path.join(RESULTS_DIR, "service_dspop_flood_mitigation_PH_conservation_inf_ssp245.tif")
    DSPOP_SERVICE_FLOOD_MITIGATION_PH_RESTORATION_SSP245 = os.path.join(RESULTS_DIR, "service_dspop_flood_mitigation_PH_restoration_ssp245.tif")
    DSPOP_SERVICE_RECHARGE_IDN_CONSERVATION_INF_SSP245 = os.path.join(RESULTS_DIR, "service_dspop_recharge_IDN_conservation_inf_ssp245.tif")
    ROAD_SERVICE_FLOOD_MITIGATION_IDN_CONSERVATION_INF_SSP245 = os.path.join(RESULTS_DIR, "service_road_flood_mitigation_IDN_conservation_inf_ssp245.tif")
    ROAD_SERVICE_FLOOD_MITIGATION_IDN_RESTORATION_SSP245 = os.path.join(RESULTS_DIR, "service_road_flood_mitigation_IDN_restoration_ssp245.tif")
    ROAD_SERVICE_FLOOD_MITIGATION_PH_CONSERVATION_INF_SSP245 = os.path.join(RESULTS_DIR, "service_road_flood_mitigation_PH_conservation_inf_ssp245.tif")
    ROAD_SERVICE_FLOOD_MITIGATION_PH_RESTORATION_SSP245 = os.path.join(RESULTS_DIR, "service_road_flood_mitigation_PH_restoration_ssp245.tif")
    ROAD_SERVICE_RECHARGE_IDN_CONSERVATION_INF_SSP245 = os.path.join(RESULTS_DIR, "service_road_recharge_IDN_conservation_inf_ssp245.tif")

    DSPOP_SERVICE_RECHARGE_IDN_RESTORATION_SSP245 = os.path.join(RESULTS_DIR, "service_dspop_recharge_IDN_restoration_ssp245.tif")
    DSPOP_SERVICE_RECHARGE_PH_CONSERVATION_INF_SSP245 = os.path.join(RESULTS_DIR, "service_dspop_recharge_PH_conservation_inf_ssp245.tif")
    DSPOP_SERVICE_RECHARGE_PH_RESTORATION_SSP245 = os.path.join(RESULTS_DIR, "service_dspop_recharge_PH_restoration_ssp245.tif")
    DSPOP_SERVICE_SEDIMENT_IDN_CONSERVATION_INF_SSP245 = os.path.join(RESULTS_DIR, "service_dspop_sediment_IDN_conservation_inf_ssp245.tif")
    DSPOP_SERVICE_SEDIMENT_IDN_RESTORATION_SSP245 = os.path.join(RESULTS_DIR, "service_dspop_sediment_IDN_restoration_ssp245.tif")
    DSPOP_SERVICE_SEDIMENT_PH_CONSERVATION_INF_SSP245 = os.path.join(RESULTS_DIR, "service_dspop_sediment_PH_conservation_inf_ssp245.tif")
    DSPOP_SERVICE_SEDIMENT_PH_RESTORATION_SSP245 = os.path.join(RESULTS_DIR, "service_dspop_sediment_PH_restoration_ssp245.tif")
    ROAD_SERVICE_RECHARGE_IDN_RESTORATION_SSP245 = os.path.join(RESULTS_DIR, "service_road_recharge_IDN_restoration_ssp245.tif")
    ROAD_SERVICE_RECHARGE_PH_CONSERVATION_INF_SSP245 = os.path.join(RESULTS_DIR, "service_road_recharge_PH_conservation_inf_ssp245.tif")
    ROAD_SERVICE_RECHARGE_PH_RESTORATION_SSP245 = os.path.join(RESULTS_DIR, "service_road_recharge_PH_restoration_ssp245.tif")
    ROAD_SERVICE_SEDIMENT_IDN_CONSERVATION_INF_SSP245 = os.path.join(RESULTS_DIR, "service_road_sediment_IDN_conservation_inf_ssp245.tif")
    ROAD_SERVICE_SEDIMENT_IDN_RESTORATION_SSP245 = os.path.join(RESULTS_DIR, "service_road_sediment_IDN_restoration_ssp245.tif")
    ROAD_SERVICE_SEDIMENT_PH_CONSERVATION_INF_SSP245 = os.path.join(RESULTS_DIR, "service_road_sediment_PH_conservation_inf_ssp245.tif")
    ROAD_SERVICE_SEDIMENT_PH_RESTORATION_SSP245 = os.path.join(RESULTS_DIR, "service_road_sediment_PH_restoration_ssp245.tif")


    # service first then beneficiary after

    # CHECK W BCK:
    # x diff between DSPOP and ROAD 'service_...' output files
    # x check that "diff" is an output to a multiply and that the filename makes sense

    SUBTRACT_RASTER_SET = [
        (r"D:\repositories\ndr_sdr_global\wwf_IDN_baseline_historical_climate\stitched_sed_export_wwf_IDN_baseline_historical_climate.tif",
         r"D:\repositories\ndr_sdr_global\wwf_IDN_restoration_historical_climate\stitched_sed_export_wwf_IDN_restoration_historical_climate.tif",
         DIFF_SEDIMENT_IDN_RESTORATION),
        (r"D:\repositories\ndr_sdr_global\wwf_IDN_baseline_ssp245_climate\stitched_sed_export_wwf_IDN_baseline_ssp245_climate.tif",
         r"D:\repositories\ndr_sdr_global\wwf_IDN_restoration_ssp245_climate\stitched_sed_export_wwf_IDN_restoration_ssp245_climate.tif",
         DIFF_SEDIMENT_IDN_RESTORATION_SSP245),
        (r"D:\repositories\ndr_sdr_global\wwf_IDN_infra_historical_climate\stitched_sed_export_wwf_IDN_infra_historical_climate.tif",
         r"D:\repositories\ndr_sdr_global\wwf_IDN_baseline_historical_climate\stitched_sed_export_wwf_IDN_baseline_historical_climate.tif",
         DIFF_SEDIMENT_IDN_CONSERVATION_INF),
        (r"D:\repositories\ndr_sdr_global\wwf_IDN_infra_ssp245_climate\stitched_sed_export_wwf_IDN_infra_ssp245_climate.tif",
         r"D:\repositories\ndr_sdr_global\wwf_IDN_baseline_ssp245_climate\stitched_sed_export_wwf_IDN_baseline_ssp245_climate.tif",
         DIFF_SEDIMENT_IDN_CONSERVATION_INF_SSP245),
        (r"D:\repositories\ndr_sdr_global\wwf_PH_baseline_historical_climate\stitched_sed_export_wwf_PH_baseline_historical_climate.tif",
         r"D:\repositories\ndr_sdr_global\wwf_PH_restoration_historical_climate\stitched_sed_export_wwf_PH_restoration_historical_climate.tif",
         DIFF_SEDIMENT_PH_RESTORATION),
        (r"D:\repositories\ndr_sdr_global\wwf_PH_baseline_ssp245_climate\stitched_sed_export_wwf_PH_baseline_ssp245_climate.tif",
         r"D:\repositories\ndr_sdr_global\wwf_PH_restoration_ssp245_climate\stitched_sed_export_wwf_PH_restoration_ssp245_climate.tif",
         DIFF_SEDIMENT_PH_RESTORATION_SSP245),
        (r"D:\repositories\ndr_sdr_global\wwf_PH_infra_historical_climate\stitched_sed_export_wwf_PH_infra_historical_climate.tif",
         r"D:\repositories\ndr_sdr_global\wwf_PH_baseline_historical_climate\stitched_sed_export_wwf_PH_baseline_historical_climate.tif",
         DIFF_SEDIMENT_PH_CONSERVATION_INF),
        (r"D:\repositories\ndr_sdr_global\wwf_PH_infra_ssp245_climate\stitched_sed_export_wwf_PH_infra_ssp245_climate.tif",
         r"D:\repositories\ndr_sdr_global\wwf_PH_baseline_ssp245_climate\stitched_sed_export_wwf_PH_baseline_ssp245_climate.tif",
         DIFF_SEDIMENT_PH_CONSERVATION_INF_SSP245),
        (r"D:\repositories\swy_global\workspace_swy_wwf_IDN_restoration_historical_climate\B_wwf_IDN_restoration_historical_climate.tif",
         r"D:\repositories\swy_global\workspace_swy_wwf_IDN_baseline_historical_climate\B_wwf_IDN_baseline_historical_climate.tif",
         DIFF_RECHARGE_IDN_RESTORATION),
        (r"D:\repositories\swy_global\workspace_swy_wwf_IDN_baseline_historical_climate\QF_wwf_IDN_baseline_historical_climate.tif",
         r"D:\repositories\swy_global\workspace_swy_wwf_IDN_restoration_historical_climate\QF_wwf_IDN_restoration_historical_climate.tif",
         DIFF_QUICKFLOW_IDN_RESTORATION),
        (r"D:\repositories\swy_global\workspace_swy_wwf_IDN_restoration_ssp245_climate10\B_wwf_IDN_restoration_ssp245_climate10.tif",
         r"D:\repositories\swy_global\workspace_swy_wwf_IDN_baseline_ssp245_climate10\B_wwf_IDN_baseline_ssp245_climate10.tif",
         DIFF_RECHARGE_IDN_RESTORATION_SSP245),
        (r"D:\repositories\swy_global\workspace_swy_wwf_IDN_baseline_ssp245_climate90\QF_wwf_IDN_baseline_ssp245_climate90.tif",
         r"D:\repositories\swy_global\workspace_swy_wwf_IDN_restoration_ssp245_climate90\QF_wwf_IDN_restoration_ssp245_climate90.tif",
         DIFF_QUICKFLOW_IDN_RESTORATION_SSP245),
        (r"D:\repositories\swy_global\workspace_swy_wwf_IDN_baseline_historical_climate\B_wwf_IDN_baseline_historical_climate.tif",
         r"D:\repositories\swy_global\workspace_swy_wwf_IDN_infra_historical_climate\B_wwf_IDN_infra_historical_climate.tif",
         DIFF_RECHARGE_IDN_CONSERVATION_INF),
        (r"D:\repositories\swy_global\workspace_swy_wwf_IDN_infra_historical_climate\QF_wwf_IDN_infra_historical_climate.tif",
         r"D:\repositories\swy_global\workspace_swy_wwf_IDN_baseline_historical_climate\QF_wwf_IDN_baseline_historical_climate.tif",
         DIFF_QUICKFLOW_IDN_CONSERVATION_INF),
        (r"D:\repositories\swy_global\workspace_swy_wwf_IDN_baseline_ssp245_climate10\B_wwf_IDN_baseline_ssp245_climate10.tif",
         r"D:\repositories\swy_global\workspace_swy_wwf_IDN_infra_ssp245_climate10\B_wwf_IDN_infra_ssp245_climate10.tif",
         DIFF_RECHARGE_IDN_CONSERVATION_INF_SSP245),
        (r"D:\repositories\swy_global\workspace_swy_wwf_IDN_infra_ssp245_climate90\QF_wwf_IDN_infra_ssp245_climate90.tif",
         r"D:\repositories\swy_global\workspace_swy_wwf_IDN_baseline_ssp245_climate90\QF_wwf_IDN_baseline_ssp245_climate90.tif",
         DIFF_QUICKFLOW_IDN_CONSERVATION_INF_SSP245),
        (r"D:\repositories\swy_global\workspace_swy_wwf_PH_restoration_historical_climate\B_wwf_PH_restoration_historical_climate.tif",
         r"D:\repositories\swy_global\workspace_swy_wwf_PH_baseline_historical_climate\B_wwf_PH_baseline_historical_climate.tif",
         DIFF_RECHARGE_PH_RESTORATION),
        (r"D:\repositories\swy_global\workspace_swy_wwf_PH_baseline_historical_climate\QF_wwf_PH_baseline_historical_climate.tif",
         r"D:\repositories\swy_global\workspace_swy_wwf_PH_restoration_historical_climate\QF_wwf_PH_restoration_historical_climate.tif",
         DIFF_QUICKFLOW_PH_RESTORATION),
        (r"D:\repositories\swy_global\workspace_swy_wwf_PH_restoration_ssp245_climate10\B_wwf_PH_restoration_ssp245_climate10.tif",
         r"D:\repositories\swy_global\workspace_swy_wwf_PH_baseline_ssp245_climate10\B_wwf_PH_baseline_ssp245_climate10.tif",
         DIFF_RECHARGE_PH_RESTORATION_SSP245),
        (r"D:\repositories\swy_global\workspace_swy_wwf_PH_baseline_ssp245_climate90\QF_wwf_PH_baseline_ssp245_climate90.tif",
         r"D:\repositories\swy_global\workspace_swy_wwf_PH_restoration_ssp245_climate90\QF_wwf_PH_restoration_ssp245_climate90.tif",
         DIFF_QUICKFLOW_PH_RESTORATION_SSP245),
        (r"D:\repositories\swy_global\workspace_swy_wwf_PH_baseline_historical_climate\B_wwf_PH_baseline_historical_climate.tif",
         r"D:\repositories\swy_global\workspace_swy_wwf_PH_infra_historical_climate\B_wwf_PH_infra_historical_climate.tif",
         DIFF_RECHARGE_PH_CONSERVATION_INF),
        (r"D:\repositories\swy_global\workspace_swy_wwf_PH_infra_historical_climate\QF_wwf_PH_infra_historical_climate.tif",
         r"D:\repositories\swy_global\workspace_swy_wwf_PH_baseline_historical_climate\QF_wwf_PH_baseline_historical_climate.tif",
         DIFF_QUICKFLOW_PH_CONSERVATION_INF),
        (r"D:\repositories\swy_global\workspace_swy_wwf_PH_baseline_ssp245_climate10\B_wwf_PH_baseline_ssp245_climate10.tif",
         r"D:\repositories\swy_global\workspace_swy_wwf_PH_infra_ssp245_climate10\B_wwf_PH_infra_ssp245_climate10.tif",
         DIFF_RECHARGE_PH_CONSERVATION_INF_SSP245),
        (r"D:\repositories\swy_global\workspace_swy_wwf_PH_infra_ssp245_climate90\QF_wwf_PH_infra_ssp245_climate90.tif",
         r"D:\repositories\swy_global\workspace_swy_wwf_PH_baseline_ssp245_climate90\QF_wwf_PH_baseline_ssp245_climate90.tif",
         DIFF_QUICKFLOW_PH_CONSERVATION_INF_SSP245)]

    MULTIPLY_RASTER_SET = [
        (r"D:\repositories\wwf-sipa\flood_risk_workspace\idn_flood_risk_weighted_flood_risk_md5_996a78.tif",
         r"D:\repositories\swy_global\workspace_swy_wwf_IDN_baseline_historical_climate\QF_wwf_IDN_baseline_historical_climate.tif",
         FLOOD_MITIGATION_IDN_BASELINE_HISTORICAL_CLIMATE),
        (r"D:\repositories\wwf-sipa\flood_risk_workspace\idn_flood_risk_weighted_flood_risk_md5_996a78.tif",
         DIFF_QUICKFLOW_IDN_CONSERVATION_INF,
         DIFF_FLOOD_MITIGATION_IDN_CONSERVATION_INF),
        (r"D:\repositories\wwf-sipa\flood_risk_workspace\idn_flood_risk_weighted_flood_risk_md5_996a78.tif",
         DIFF_QUICKFLOW_IDN_CONSERVATION_INF_SSP245,
         DIFF_FLOOD_MITIGATION_IDN_CONSERVATION_INF_SSP245),
        (r"D:\repositories\wwf-sipa\flood_risk_workspace\idn_flood_risk_weighted_flood_risk_md5_996a78.tif",
         DIFF_QUICKFLOW_IDN_RESTORATION,
         DIFF_FLOOD_MITIGATION_IDN_RESTORATION),
        (r"D:\repositories\wwf-sipa\flood_risk_workspace\idn_flood_risk_weighted_flood_risk_md5_996a78.tif",
         DIFF_QUICKFLOW_IDN_RESTORATION_SSP245,
         DIFF_FLOOD_MITIGATION_IDN_RESTORATION_SSP245),
        (r"D:\repositories\wwf-sipa\flood_risk_workspace\ph_flood_risk_weighted_flood_risk_md5_4d2475.tif",
         r"D:\repositories\swy_global\workspace_swy_wwf_PH_baseline_historical_climate\QF_wwf_PH_baseline_historical_climate.tif",
         FLOOD_MITIGATION_PH_BASELINE_HISTORICAL_CLIMATE),
        (r"D:\repositories\wwf-sipa\flood_risk_workspace\ph_flood_risk_weighted_flood_risk_md5_4d2475.tif",
         DIFF_QUICKFLOW_PH_CONSERVATION_INF,
         DIFF_FLOOD_MITIGATION_PH_CONSERVATION_INF),
        (r"D:\repositories\wwf-sipa\flood_risk_workspace\ph_flood_risk_weighted_flood_risk_md5_4d2475.tif",
         DIFF_QUICKFLOW_PH_CONSERVATION_INF_SSP245,
         DIFF_FLOOD_MITIGATION_PH_CONSERVATION_INF_SSP245),
        (r"D:\repositories\wwf-sipa\flood_risk_workspace\ph_flood_risk_weighted_flood_risk_md5_4d2475.tif",
         DIFF_QUICKFLOW_PH_RESTORATION,
         DIFF_FLOOD_MITIGATION_PH_RESTORATION),
        (r"D:\repositories\wwf-sipa\flood_risk_workspace\ph_flood_risk_weighted_flood_risk_md5_4d2475.tif",
         DIFF_QUICKFLOW_PH_RESTORATION_SSP245,
         DIFF_FLOOD_MITIGATION_PH_RESTORATION_SSP245),
        (DIFF_RECHARGE_IDN_RESTORATION,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_dspop_benes_md5_e4d2c4.tif",
         DSPOP_SERVICE_RECHARGE_IDN_RESTORATION),
        (DIFF_FLOOD_MITIGATION_IDN_RESTORATION,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_dspop_benes_md5_e4d2c4.tif",
         DSPOP_SERVICE_FLOOD_MITIGATION_IDN_RESTORATION),
        (DIFF_SEDIMENT_IDN_RESTORATION,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_dspop_benes_md5_e4d2c4.tif",
         DSPOP_SERVICE_SEDIMENT_IDN_RESTORATION),
        (DIFF_RECHARGE_IDN_RESTORATION,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_road2019_benes_md5_8ec2cd.tif",
         ROAD_SERVICE_RECHARGE_IDN_RESTORATION),
        (DIFF_FLOOD_MITIGATION_IDN_RESTORATION,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_road2019_benes_md5_8ec2cd.tif",
         ROAD_SERVICE_FLOOD_MITIGATION_IDN_RESTORATION),
        (DIFF_SEDIMENT_IDN_RESTORATION,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_road2019_benes_md5_8ec2cd.tif",
         ROAD_SERVICE_SEDIMENT_IDN_RESTORATION),
        (DIFF_RECHARGE_PH_RESTORATION,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_dspop_benes_md5_028732.tif",
         DSPOP_SERVICE_RECHARGE_PH_RESTORATION),
        (DIFF_FLOOD_MITIGATION_PH_RESTORATION,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_dspop_benes_md5_028732.tif",
         DSPOP_SERVICE_FLOOD_MITIGATION_PH_RESTORATION),
        (DIFF_SEDIMENT_PH_RESTORATION,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_dspop_benes_md5_028732.tif",
         DSPOP_SERVICE_SEDIMENT_PH_RESTORATION),
        (DIFF_RECHARGE_PH_RESTORATION,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_road2019_benes_md5_870a6c.tif",
         ROAD_SERVICE_RECHARGE_PH_RESTORATION),
        (DIFF_FLOOD_MITIGATION_PH_RESTORATION,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_road2019_benes_md5_870a6c.tif",
         ROAD_SERVICE_FLOOD_MITIGATION_PH_RESTORATION),
        (DIFF_SEDIMENT_PH_RESTORATION,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_road2019_benes_md5_870a6c.tif",
         ROAD_SERVICE_SEDIMENT_PH_RESTORATION),
        (DIFF_RECHARGE_IDN_CONSERVATION_INF,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_dspop_benes_md5_e4d2c4.tif",
         DSPOP_SERVICE_RECHARGE_IDN_CONSERVATION_INF),
        (DIFF_FLOOD_MITIGATION_IDN_CONSERVATION_INF,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_dspop_benes_md5_e4d2c4.tif",
         DSPOP_SERVICE_FLOOD_MITIGATION_IDN_CONSERVATION_INF),
        (DIFF_SEDIMENT_IDN_CONSERVATION_INF,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_dspop_benes_md5_e4d2c4.tif",
         DSPOP_SERVICE_SEDIMENT_IDN_CONSERVATION_INF),
        (DIFF_RECHARGE_IDN_CONSERVATION_INF,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_road2019_benes_md5_8ec2cd.tif",
         ROAD_SERVICE_RECHARGE_IDN_CONSERVATION_INF),
        (DIFF_FLOOD_MITIGATION_IDN_CONSERVATION_INF,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_road2019_benes_md5_8ec2cd.tif",
         ROAD_SERVICE_FLOOD_MITIGATION_IDN_CONSERVATION_INF),
        (DIFF_SEDIMENT_IDN_CONSERVATION_INF,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_road2019_benes_md5_8ec2cd.tif",
         ROAD_SERVICE_SEDIMENT_IDN_CONSERVATION_INF),
        (DIFF_RECHARGE_PH_CONSERVATION_INF,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_dspop_benes_md5_028732.tif",
         DSPOP_SERVICE_RECHARGE_PH_CONSERVATION_INF),
        (DIFF_FLOOD_MITIGATION_PH_CONSERVATION_INF,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_dspop_benes_md5_028732.tif",
         DSPOP_SERVICE_FLOOD_MITIGATION_PH_CONSERVATION_INF),
        (DIFF_SEDIMENT_PH_CONSERVATION_INF,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_dspop_benes_md5_028732.tif",
         DSPOP_SERVICE_SEDIMENT_PH_CONSERVATION_INF),
        (DIFF_RECHARGE_PH_CONSERVATION_INF,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_road2019_benes_md5_870a6c.tif",
         ROAD_SERVICE_RECHARGE_PH_CONSERVATION_INF),
        (DIFF_FLOOD_MITIGATION_PH_CONSERVATION_INF,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_road2019_benes_md5_870a6c.tif",
         ROAD_SERVICE_FLOOD_MITIGATION_PH_CONSERVATION_INF),
        (DIFF_SEDIMENT_PH_CONSERVATION_INF,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_road2019_benes_md5_870a6c.tif",
         ROAD_SERVICE_SEDIMENT_PH_CONSERVATION_INF),



        (DIFF_RECHARGE_IDN_RESTORATION_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_dspop_benes_md5_e4d2c4.tif",
         DSPOP_SERVICE_RECHARGE_IDN_RESTORATION_SSP245),
        (DIFF_FLOOD_MITIGATION_IDN_RESTORATION_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_dspop_benes_md5_e4d2c4.tif",
         DSPOP_SERVICE_FLOOD_MITIGATION_IDN_RESTORATION_SSP245),
        (DIFF_SEDIMENT_IDN_RESTORATION_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_dspop_benes_md5_e4d2c4.tif",
         DSPOP_SERVICE_SEDIMENT_IDN_RESTORATION_SSP245),
        (DIFF_RECHARGE_IDN_RESTORATION_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_road2019_benes_md5_8ec2cd.tif",
         ROAD_SERVICE_RECHARGE_IDN_RESTORATION_SSP245),
        (DIFF_FLOOD_MITIGATION_IDN_RESTORATION_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_road2019_benes_md5_8ec2cd.tif",
         ROAD_SERVICE_FLOOD_MITIGATION_IDN_RESTORATION_SSP245),
        (DIFF_SEDIMENT_IDN_RESTORATION_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_road2019_benes_md5_8ec2cd.tif",
         ROAD_SERVICE_SEDIMENT_IDN_RESTORATION_SSP245),
        (DIFF_RECHARGE_PH_RESTORATION_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_dspop_benes_md5_028732.tif",
         DSPOP_SERVICE_RECHARGE_PH_RESTORATION_SSP245),
        (DIFF_FLOOD_MITIGATION_PH_RESTORATION_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_dspop_benes_md5_028732.tif",
         DSPOP_SERVICE_FLOOD_MITIGATION_PH_RESTORATION_SSP245),
        (DIFF_SEDIMENT_PH_RESTORATION_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_dspop_benes_md5_028732.tif",
         DSPOP_SERVICE_SEDIMENT_PH_RESTORATION_SSP245),
        (DIFF_RECHARGE_PH_RESTORATION_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_road2019_benes_md5_870a6c.tif",
         ROAD_SERVICE_RECHARGE_PH_RESTORATION_SSP245),
        (DIFF_FLOOD_MITIGATION_PH_RESTORATION_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_road2019_benes_md5_870a6c.tif",
         ROAD_SERVICE_FLOOD_MITIGATION_PH_RESTORATION_SSP245),
        (DIFF_SEDIMENT_PH_RESTORATION_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_road2019_benes_md5_870a6c.tif",
         ROAD_SERVICE_SEDIMENT_PH_RESTORATION_SSP245),
        (DIFF_RECHARGE_IDN_CONSERVATION_INF_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_dspop_benes_md5_e4d2c4.tif",
         DSPOP_SERVICE_RECHARGE_IDN_CONSERVATION_INF_SSP245),
        (DIFF_FLOOD_MITIGATION_IDN_CONSERVATION_INF_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_dspop_benes_md5_e4d2c4.tif",
         DSPOP_SERVICE_FLOOD_MITIGATION_IDN_CONSERVATION_INF_SSP245),
        (DIFF_SEDIMENT_IDN_CONSERVATION_INF_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_dspop_benes_md5_e4d2c4.tif",
         DSPOP_SERVICE_SEDIMENT_IDN_CONSERVATION_INF_SSP245),
        (DIFF_RECHARGE_IDN_CONSERVATION_INF_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_road2019_benes_md5_8ec2cd.tif",
         ROAD_SERVICE_RECHARGE_IDN_CONSERVATION_INF_SSP245),
        (DIFF_FLOOD_MITIGATION_IDN_CONSERVATION_INF_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_road2019_benes_md5_8ec2cd.tif",
         ROAD_SERVICE_FLOOD_MITIGATION_IDN_CONSERVATION_INF_SSP245),
        (DIFF_SEDIMENT_IDN_CONSERVATION_INF_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_idn_downstream_road2019_benes_md5_8ec2cd.tif",
         ROAD_SERVICE_SEDIMENT_IDN_CONSERVATION_INF_SSP245),
        (DIFF_RECHARGE_PH_CONSERVATION_INF_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_dspop_benes_md5_028732.tif",
         DSPOP_SERVICE_RECHARGE_PH_CONSERVATION_INF_SSP245),
        (DIFF_FLOOD_MITIGATION_PH_CONSERVATION_INF_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_dspop_benes_md5_028732.tif",
         DSPOP_SERVICE_FLOOD_MITIGATION_PH_CONSERVATION_INF_SSP245),
        (DIFF_SEDIMENT_PH_CONSERVATION_INF_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_dspop_benes_md5_028732.tif",
         DSPOP_SERVICE_SEDIMENT_PH_CONSERVATION_INF_SSP245),
        (DIFF_RECHARGE_PH_CONSERVATION_INF_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_road2019_benes_md5_870a6c.tif",
         ROAD_SERVICE_RECHARGE_PH_CONSERVATION_INF_SSP245),
        (DIFF_FLOOD_MITIGATION_PH_CONSERVATION_INF_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_road2019_benes_md5_870a6c.tif",
         ROAD_SERVICE_FLOOD_MITIGATION_PH_CONSERVATION_INF_SSP245),
        (DIFF_SEDIMENT_PH_CONSERVATION_INF_SSP245,
         r"D:\repositories\wwf-sipa\downstream_beneficiary_workspace\num_of_downstream_beneficiaries_per_pixel_ph_downstream_road2019_benes_md5_870a6c.tif",
         ROAD_SERVICE_SEDIMENT_PH_CONSERVATION_INF_SSP245)
        ]

    subtract_output_set = set([t[2] for t in SUBTRACT_RASTER_SET])
    multiply_output_set = set([t[2] for t in MULTIPLY_RASTER_SET])
    path_count = collections.defaultdict(int)
    for p in MULTIPLY_RASTER_SET:
        path = p[2]
        path_count[path] += 1
        if path_count[path] > 1:
            print(f'duplicate: {path}')

    print(len(subtract_output_set) == len(SUBTRACT_RASTER_SET))
    print(len(multiply_output_set) == len(MULTIPLY_RASTER_SET))
    print(len(set(MULTIPLY_RASTER_SET)) == len(MULTIPLY_RASTER_SET))

    for raster_a_in, raster_b_in, target_raster in SUBTRACT_RASTER_SET+MULTIPLY_RASTER_SET:
        for p in [raster_a_in, raster_b_in]:
            if not os.path.exists(p) and RESULTS_DIR not in p:
                print(f'input path does not exist: {p}')

    task_graph = taskgraph.TaskGraph(RESULTS_DIR, os.cpu_count(), 15.0)

    service_raster_path_list = []
    task_set = {}
    for raster_a_path, raster_b_path, target_raster_path, op_str in \
            [t+('-',) for t in SUBTRACT_RASTER_SET]+\
            [t+('*',) for t in MULTIPLY_RASTER_SET]:

        dependent_task_list = []
        for p in [raster_a_path, raster_b_path]:
            if p in task_set:
                dependent_task_list.append(task_set[p])
        op_task = task_graph.add_task(
            func=raster_op,
            args=(op_str, raster_a_path, raster_b_path, target_raster_path),
            target_path_list=[target_raster_path],
            dependent_task_list=dependent_task_list,
            task_name=f'calcualte {target_raster_path}')
        if target_raster_path in task_set:
            raise ValueError(f'calculating a result that we alreayd calculated {target_raster_path}')
        task_set[target_raster_path] = op_task
        if 'service' in target_raster_path:
            service_raster_path_list.append((target_raster_path, op_task))

    # ASK BCK: gte-75 gte-90 means top 25 top 10 so only 25 or 10% are selected
    # :::: call python mask_by_percentile.py D:\repositories\wwf-sipa\final_results\service_*.tif gte-75-percentile_[file_name]_gte75.tif gte-90-percentile_[file_name]_gte90.tif
    percentile_task_list = []
    for service_path, service_task in service_raster_path_list:
        percentile_task = task_graph.add_task(
            func=make_top_nth_percentile_masks,
            args=(
                service_path,
                top_percentile_list,
                os.path.join(RESULTS_DIR, 'top_{percentile}th_percentile_' + os.path.basename(service_path))),
            dependent_task_list=[service_task],
            store_result=True,
            task_name=f'percentile for {service_path}')
        percentile_task_list.append((service_path, percentile_task))
    task_graph.join()
    task_graph.close()
    LOGGER.info(f'all done! results in {RESULTS_DIR}')
    percentile_raster_list = []
    for service_path, percentile_task in percentile_task_list:
        local_percentile_rasters = percentile_task.get()
        LOGGER.info(f'percentile for {service_path} is {percentile_task.get()}')
        percentile_raster_list.extend(local_percentile_rasters)

    # :::: then add_sub_missing_as_zero for all the percentile_masks for each scenario so we can see the pixels that are in the top 25 or top 10
        #percent for all services vs. multiple services vs. just for one
    # we'll need to:
    #   1) segment out what percentile it is
    #   2) segment out which scenario it is
    #   3) segiment out which service it is
    #   4) segment out which climate it is
    percentile_groups = collections.defaultdict(list)
    for percentile_raster_path in local_percentile_rasters:
        for percentile in top_percentile_list:
            if str(percentile)+'th' in percentile_raster_path:
                break
        for country in country_list:
            if country not in percentile_raster_path:
                break
        for scenario in scenario_list:
            if scenario not in percentile_raster_path:
                break
        for beneficiary in beneficiary_list:
            if beneficiary not in percentile_raster_path:
                break
        for climate in climate_list:
            if climate not in percentile_raster_path:
                break
        percentile_groups[f'{percentile}th_{country}_{scenario}_{beneficiary}_{climate}'].append(percentile_raster_path)

    LOGGER.debug(f'these are the percentile groups: {percentile_groups}')
    return

    # TODO: okay, here's where I left off:
    #   * i'm debugging why my percentile groups don't have groups of 3 services in therm, in the process of that I identified that
    #       we hadn't broken it down by climate for services
    #       * along these lines now all the climate services and percentiles are running too
    #   * after I figure the above out i should be able to add the masks for the 10th/25th percentiles to get service coverage rasters
    #   * after that I wawnt to aggregate those service coverage rasters to the ADM3 and 4 polygons
    #   * after that load ALL the rasters and the polygons onto a viewer THEN DONE!


    # for percentile_raster_group_list, subgroup_id in percentile_groups.items():
    #     overlap_raster_path = os.path.join(RESULTS_DIR, f'overlap_{subgroup_id}.tif')
    #     task_graph.add_task(
    #         func=sum_masks,
    #         args=(percentile_raster_group_list, overlap_raster_path),
    #         target_path_list=[overlap_raster_path],
    #         task_name=f'overlap for {overlap_raster_path}')
    #     pass

    # :::: repeat the last three steps above for climate scenarios *ssp245 and see what the % overlap is (how much does the portfolio change
        # under future climate?)
    # :::: then cog and put everything on viewer (all the diff_ rasters and service_ rasters and the individual percentile masks and the
        # composite/added up percentile mask)

    # (the MULT rasters, these are the diff [service] rasters scaled by beneficiaries) diff x benes x services (4) x scenarios (2) x climate (2)
    # we then take percentile maps top 25% 10% of these
    # then we need to reduce -- what is the overlap of services of the top 25%/10? how does it reduce to ADM units?
        # reduce those ADMs to be total amount and proportional by area amount for "where DO I do seomthing or where DON'T I do something?"


if __name__ == '__main__':
    main()
