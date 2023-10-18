import collections
import os


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


if __name__ == '__main__':
    main()
