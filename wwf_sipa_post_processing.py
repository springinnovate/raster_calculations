import collections
import os
import logging
import sys
import tempfile
import shutil

from ecoshard import taskgraph
from ecoshard import geoprocessing
import numpy

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


def do_op(op_str, raster_path_a, raster_path_b, target_raster_path, target_nodata=None, target_datatype=None):
    base_raster_list = [
        raster_path_a,
        raster_path_b]
    working_dir = tempfile.mkdtemp(
        dir=os.path.dirname(target_raster_path))
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

    geoprocessing.raster_calculator(
        [(path, 1) for path in aligned_target_raster_path_list],
        _op(a_nodata, b_nodata, target_nodata),
        target_raster_path, target_datatype, target_nodata,
        raster_driver_creation_tuple=('COG', (
            'TILED=YES', 'BIGTIFF=YES', 'COMPRESS=LZW',
            'BLOCKXSIZE=256', 'BLOCKYSIZE=256')))
    shutil.rmtree(working_dir)


def main():
    RESULTS_DIR = 'D:\\repositories\\wwf-sipa\\final_results'

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

    DSPOP_SERVICE_FLOOD_MITIGATION_IDN_CONSERVATION_INF = os.path.join(RESULTS_DIR, "dspop_service_flood_mitigation_IDN_conservation_inf.tif")
    DSPOP_SERVICE_FLOOD_MITIGATION_IDN_RESTORATION = os.path.join(RESULTS_DIR, "dspop_service_flood_mitigation_IDN_restoration.tif")
    DSPOP_SERVICE_FLOOD_MITIGATION_PH_CONSERVATION_INF = os.path.join(RESULTS_DIR, "dspop_service_flood_mitigation_PH_conservation_inf.tif")
    DSPOP_SERVICE_FLOOD_MITIGATION_PH_RESTORATION = os.path.join(RESULTS_DIR, "dspop_service_flood_mitigation_PH_restoration.tif")
    DSPOP_SERVICE_RECHARGE_IDN_CONSERVATION_INF = os.path.join(RESULTS_DIR, "dspop_service_recharge_IDN_conservation_inf.tif")

    ROAD_SERVICE_FLOOD_MITIGATION_IDN_CONSERVATION_INF = os.path.join(RESULTS_DIR, "road_service_flood_mitigation_IDN_conservation_inf.tif")
    ROAD_SERVICE_FLOOD_MITIGATION_IDN_RESTORATION = os.path.join(RESULTS_DIR, "road_service_flood_mitigation_IDN_restoration.tif")
    ROAD_SERVICE_FLOOD_MITIGATION_PH_CONSERVATION_INF = os.path.join(RESULTS_DIR, "road_service_flood_mitigation_PH_conservation_inf.tif")
    ROAD_SERVICE_FLOOD_MITIGATION_PH_RESTORATION = os.path.join(RESULTS_DIR, "road_service_flood_mitigation_PH_restoration.tif")
    ROAD_SERVICE_RECHARGE_IDN_CONSERVATION_INF = os.path.join(RESULTS_DIR, "road_service_recharge_IDN_conservation_inf.tif")

    DSPOP_SERVICE_RECHARGE_IDN_RESTORATION = os.path.join(RESULTS_DIR, "dspop_service_recharge_IDN_restoration.tif")
    DSPOP_SERVICE_RECHARGE_PH_CONSERVATION_INF = os.path.join(RESULTS_DIR, "dspop_service_recharge_PH_conservation_inf.tif")
    DSPOP_SERVICE_RECHARGE_PH_RESTORATION = os.path.join(RESULTS_DIR, "dspop_service_recharge_PH_restoration.tif")
    DSPOP_SERVICE_SEDIMENT_IDN_CONSERVATION_INF = os.path.join(RESULTS_DIR, "dspop_service_sediment_IDN_conservation_inf.tif")
    DSPOP_SERVICE_SEDIMENT_IDN_RESTORATION = os.path.join(RESULTS_DIR, "dspop_service_sediment_IDN_restoration.tif")
    DSPOP_SERVICE_SEDIMENT_PH_CONSERVATION_INF = os.path.join(RESULTS_DIR, "dspop_service_sediment_PH_conservation_inf.tif")
    DSPOP_SERVICE_SEDIMENT_PH_RESTORATION = os.path.join(RESULTS_DIR, "dspop_service_sediment_PH_restoration.tif")

    ROAD_SERVICE_RECHARGE_IDN_RESTORATION = os.path.join(RESULTS_DIR, "road_service_recharge_IDN_restoration.tif")
    ROAD_SERVICE_RECHARGE_PH_CONSERVATION_INF = os.path.join(RESULTS_DIR, "road_service_recharge_PH_conservation_inf.tif")
    ROAD_SERVICE_RECHARGE_PH_RESTORATION = os.path.join(RESULTS_DIR, "road_service_recharge_PH_restoration.tif")
    ROAD_SERVICE_SEDIMENT_IDN_CONSERVATION_INF = os.path.join(RESULTS_DIR, "road_service_sediment_IDN_conservation_inf.tif")
    ROAD_SERVICE_SEDIMENT_IDN_RESTORATION = os.path.join(RESULTS_DIR, "road_service_sediment_IDN_restoration.tif")
    ROAD_SERVICE_SEDIMENT_PH_CONSERVATION_INF = os.path.join(RESULTS_DIR, "road_service_sediment_PH_conservation_inf.tif")
    ROAD_SERVICE_SEDIMENT_PH_RESTORATION = os.path.join(RESULTS_DIR, "road_service_sediment_PH_restoration.tif")


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
         ROAD_SERVICE_SEDIMENT_PH_CONSERVATION_INF)]

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

    service_set = []
    for raster_a_path, raster_b_path, target_raster_path, op_str in \
            [t+('-',) for t in SUBTRACT_RASTER_SET]+\
            [t+('*',) for t in MULTIPLY_RASTER_SET]:
        dependent_task_list = []
        for p in [raster_a_path, raster_b_path]:
            dependent_task_list += p
        op_task = task_graph.add_task(
            func=do_op,
            args=(op_str, raster_a_path, raster_b_path, target_raster_path),
            target_path_list=[target_raster_path],
            dependent_task_list=dependent_task_list,
            task_name=f'calcualte {target_raster_path}')
        if 'service' in target_raster_path:
            service_set.append((target_raster_path, op_task))

    task_graph.join()
    task_graph.close()
    LOGGER.info(f'all done! results in {RESULTS_DIR}')

    # ASK BCK: gte-75 gte-90 means top 25 top 10 so only 25 or 10% are selected
    # :::: call python mask_by_percentile.py D:\repositories\wwf-sipa\final_results\service_*.tif gte-75-percentile_[file_name]_gte75.tif gte-90-percentile_[file_name]_gte90.tif
    # :::: then add_sub_missing_as_zero for all the percentile_masks for each scenario so we can see the pixels that are in the top 25 or top 10 percent for all services vs. multiple services vs. just for one
    # :::: repeat the last three steps above for climate scenarios *ssp245 and see what the % overlap is (how much does the portfolio change under future climate?)
    # :::: then cog and put everything on viewer (all the diff_ rasters and service_ rasters and the individual percentile masks and the composite/added up percentile mask)



if __name__ == '__main__':
    main()
