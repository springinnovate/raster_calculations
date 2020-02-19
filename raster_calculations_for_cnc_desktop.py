"""These calculations are for the Critical Natural Capital paper."""
import glob
import sys
import os
import logging
import multiprocessing
import datetime
import subprocess
import raster_calculations_core
from osgeo import gdal
import taskgraph

gdal.SetCacheMax(2**30)

WORKSPACE_DIR = 'CNC_workspace'
NCPUS = multiprocessing.cpu_count()
try:
    os.makedirs(WORKSPACE_DIR)
except OSError:
    pass

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


def main():
    """Write your expression here."""

    # CNC calculations


    masked_service_list = [
        {
            'expression': 'service*mask + -1*(1-mask)', #this sets all values not in the mask to nodata (in this case, -1)
            'symbol_to_path_map': {
                'mask': r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\processed_rasters_dir\14e98a7df6fa935207dbb83454612485\masked_all_nathab_esa2015.tif",
                'service': r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\processed_rasters_dir\14e98a7df6fa935207dbb83454612485\half_degree_grid_langa_19_dslv_density.tif",
            },
            'target_nodata': -1,
            'default_nan': -1,
            'target_raster_path': "realized_cultural_language_nathab.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
    ]

    for calculation in masked_service_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    masker_list = [
         {
            # the %s is a placeholder for the string we're passing it using this function that lists every number in the range and takes away the [] of the list and turns it into a string
            'expression': 'mask(raster, %s, invert=False)'%(str([]+[x for x in range(50,181)])[1:-1]),
            'symbol_to_path_map': {
                'raster': r"C:\Users\Becky\Documents\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "masked_all_nathab_esa2015.tif",
        },
        {
            # this is for masking out forest from natural habitat, for livestock production
            # this counts the >50% herbaceous / < 50% tree cover category as "not forest"; also includes lichens, mosses  and shrubland which maybe isn't totally edible by cattle either
            'expression': 'mask(raster, %s, invert=False)'%(str([x for x in range(110,154)]+[180])[1:-1]),
            'symbol_to_path_map': {
                'raster': r"C:\Users\Becky\Documents\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "masked_nathab_notforest_esa2015.tif",
        },
        {
            'expression': 'mask(raster, %s, invert=False)'%(str([x for x in range(50,111)]+[150]+[151]+[160]+[170])[1:-1]),
            'symbol_to_path_map': {
                'raster': r"C:\Users\Becky\Documents\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "masked_nathab_forest_esa2015.tif",
        },
    ]
    for masker in masker_list:
       raster_calculations_core.evaluate_calculation(
            masker, TASK_GRAPH, WORKSPACE_DIR)
    

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return
    
    #Single expression

    synthesis_index_expression = {
            'expression': 'nitrogen + sediment + pollination + timber + nwfp + grazing ',
            'symbol_to_path_map': {
                'nitrogen': "CNC_workspace/norm_by_country_masked_realized_nitrogenretention_downstream_md5_82d4e57042482eb1b92d03c0d387f501.tif",
                'nwfp': "CNC_workspace/norm_by_country_masked_realized_nwfp_masked_md5_a907048c3cc62ec51640048bb710d8d8.tif",
                'pollination': "CNC_workspace/norm_by_country_masked_realized_pollination_md5_443522f6688011fd561297e9a556629b.tif",
                'sediment': "CNC_workspace/norm_by_country_masked_realized_sedimentdeposition_downstream_md5_1613b12643898c1475c5ec3180836770.tif",
                'grazing': "CNC_workspace/norm_by_country_masked_realized_grazing_md5_19085729ae358e0e8566676c5c7aae72.tif",
                'timber': "CNC_workspace/norm_by_country_masked_realized_timber_md5_340467b17d0950d381f55cd355ae688a.tif",  
            },
            'target_nodata': -1,
            'target_raster_path': "CNC_workspace/aggregate_normbycountry_realized_ES_score_nspwog.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
            'resample_method': 'average'
    }

    raster_calculations_core.evaluate_calculation(
        synthesis_index_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()



    #loop to set thresholds

    for base_raster_path, threshold, target_raster_path in [
            #(r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\normalized_realized_pollination_md5_06f52f2854ae1c584742d587b1c31359.tif", 0.06, "top04_pollination.tif"),
            #(r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\normalized_realized_flood_md5_f1237e76a41039e22629abb85963ba16.tif", 0.05, "top30_flood.tif"),
            #(r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\normalized_realized_grazing_md5_d03b584dac965539a77bf96cba3f8096_masked_md5_db038b499342efa926c3c5815c822fe3.tif", 0.1, "top15_grazing.tif"),
            #(r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\normalized_realized_nitrogen_downstream_md5_437e1759b0f994b47add4baf76509bbe_masked_md5_ac82368cedcfc692b0440b0cc0ed7fdb.tif", 0.06, "top25_nitrogen.tif"),
            #(r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\normalized_realized_nwfp_masked_md5_754ba4d8cd0c54399fd816748a9e0091_masked_md5_f48ada73cb74cd59726b066db2f03855.tif", 0.05, "top10_nwfp.tif"),
            #(r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\normalized_realized_sediment_downstream_md5_daa86f70232c5e1a8a0efaf0b2653db2_masked_md5_6e9050a9fcf3f08925343a48208aeab8.tif", 0.09, "top05_sediment.tif"),
            #(r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\normalized_realized_timber_masked_md5_fc5ad0ff1f4702d75f204267fc90b33f_masked_md5_68df861a8e4c5cbb0e800f389690a792.tif", 0.13, "top15_timber.tif"),
            (r"C:\Users\Becky\Documents\raster_calculations\aggregate_realized_ES_score_nspwogf_md5_0ab07f38ed0290fea6142db188ae51f8.tif", 0.30, "top40_nspwogf.tif"),
            ]:

        mask_expr_dict = {
            'expression': 'raster > %f' % threshold,
            'symbol_to_path_map': {
                'raster': base_raster_path,
            },
            'target_nodata': -1,
            'target_raster_path': target_raster_path,
        }

        raster_calculations_core.evaluate_calculation(
            mask_expr_dict, TASK_GRAPH, WORKSPACE_DIR)
    

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    #looping the same mask over a bunch of rasters

    base_directory = r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace"

    masked_workspace_dir = 'masked_workspace_dir'
    ecoshard_workspace_dir = 'ecoshard_dir'
    for dirname in [masked_workspace_dir, ecoshard_workspace_dir]:
        try:
            os.makedirs(dirname)
        except OSError:
            pass

    for path in glob.glob(os.path.join(base_directory, '*.tif')):
        path_root_name = os.path.splitext(os.path.basename(path))[0]
        target_raster_path = os.path.join(
            masked_workspace_dir, '%s_masked.tif' % (path_root_name))

        remasking_expression = {
                'expression': 'mask*service',
                'symbol_to_path_map': {
                    'mask': 'masked_nathab_esa_nodata_md5_7c9acfe052cb7bdad319f011e9389fb1.tif',
                    'service': path,
                },
                'target_nodata': -1,
                'target_raster_path': target_raster_path,
                 ###file name split off from its path and its ecoshard too because it will be re-ecosharded
                'target_pixel_size': (0.002777777777778, -0.002777777777778),
            }

        raster_calculations_core.evaluate_calculation(
            remasking_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    subprocess.check_call("python -m ecoshard ./masked_workspace_dir/*.tif --hash_file --rename --buildoverviews --interpolation_method average")    
    
    TASK_GRAPH.join()
    TASK_GRAPH.close()


    # a list of expressions

    aggregate_service_list = [
        {
            'expression': 'previous_aggregate + flood + moisture',
            'symbol_to_path_map': {
                'previous_aggregate': "aggregate_potential_ES_score_nspwog_md5_afc4ad7c7337a974a7284d5f59bf4c69.tif",
                'flood': "normalized_potential_flood_md5_6b603609e55d3a17d20ea76699aaaf79.tif",
                'moisture': "normalized_potential_moisture_md5_d5396383d8a30f296988f86bb0fc0528.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "aggregate_potential_ES_score_nspwogfm.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
            'resample_method': 'average'
        },
        {
            'expression': 'previous_aggregate + flood',
            'symbol_to_path_map': {
                'previous_aggregate': "aggregate_potential_ES_score_nspwog_md5_afc4ad7c7337a974a7284d5f59bf4c69.tif",
                'flood': "normalized_potential_flood_md5_6b603609e55d3a17d20ea76699aaaf79.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "aggregate_potential_ES_score_nspwogf.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
            'resample_method': 'average'
        },
        {
            'expression': 'previous_aggregate + flood',
            'symbol_to_path_map': {
                'previous_aggregate': "aggregate_realized_ES_score_nspwog_md5_715a67e14a188826f67da5eb5c52c4da.tif",
                'flood': "normalized_realized_flood_md5_f1237e76a41039e22629abb85963ba16.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "aggregate_realized_ES_score_nspwogf.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
            'resample_method': 'average'
        },
    ]
    
    for calculation in aggregate_service_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    subprocess.check_call("python -m ecoshard aggregate_potential_ES_score_nspwogfm.tif --hash_file --rename --buildoverviews --interpolation_method average")    
   
    TASK_GRAPH.join()
    subprocess.check_call("python -m ecoshard aggregate_potential_ES_score_nspwogf.tif --hash_file --rename --buildoverviews --interpolation_method average")    

    TASK_GRAPH.join()
    subprocess.check_call("python -m ecoshard aggregate_realized_ES_score_nspwogf.tif --hash_file --rename --buildoverviews --interpolation_method average")    

    TASK_GRAPH.join()
    TASK_GRAPH.close()
    return


    normalized_service_list = [
        {
            'expression': 'service/0.12518708407878876',
            'symbol_to_path_map': {
                'service': "potential_flood_storage.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "raw_normalized_potential_flood.tif",
        },
        {
            'expression': 'service/269.3',
            'symbol_to_path_map': {
                'service': "potential_moisture_recycling.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "raw_normalized_potential_moisture.tif",
        },
        {
            'expression': 'service/ 276336.7',
            'symbol_to_path_map': {
                'service': "realized_flood_storage.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "raw_normalized_realized_flood.tif",
        },
        {
            'expression': 'service/890106050.9',
            'symbol_to_path_map': {
                'service': "realized_moisture_recycling.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "raw_normalized_realized_moisture.tif",
        },
    ]

    for calculation in normalized_service_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()

    clamping_service_list = [
        {
            'expression': '(val >= 0) * (val < 1) * val + (val >= 1)',
            'symbol_to_path_map': {
                'val': "raw_normalized_potential_flood.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "normalized_potential_flood.tif",
        },
        {
            'expression': '(val >= 0) * (val < 1) * val + (val >= 1)',
            'symbol_to_path_map': {
                'val': "raw_normalized_potential_moisture.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "normalized_potential_moisture.tif",
        },
        {
            'expression': '(val >= 0) * (val < 1) * val + (val >= 1)',
            'symbol_to_path_map': {
                'val': "raw_normalized_realized_flood.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "normalized_realized_flood.tif",
        },
        {
            'expression': '(val >= 0) * (val < 1) * val + (val >= 1)',
            'symbol_to_path_map': {
                'val': "raw_normalized_realized_moisture.tif",
            },
            'target_nodata': -1,
            'target_raster_path': "normalized_realized_moisture.tif",
        },
    ]

    for calculation in clamping_service_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)


    TASK_GRAPH.join()
    TASK_GRAPH.close()


    return #terminates at this point


    # just build overviews
    raster_calculation_list = [
        {
            'expression': 'x',
            'symbol_to_path_map': {
                'x': '../nathab_potential_pollination.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "potential_pollination.tif",
            'build_overview': True,
        },
    ]

    for calculation in raster_calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)
   
    TASK_GRAPH.join()
    TASK_GRAPH.close()

    #original masker list
    masker_list = [
         {
            # the %s is a placeholder for the string we're passing it using this function that lists every number in the range and takes away the [] of the list and turns it into a string
            'expression': 'mask(raster, %s, invert=False)'%(str([]+[x for x in range(50,181)])[1:-1]),
            'symbol_to_path_map': {
                'raster': 'https://storage.googleapis.com/ipbes-ndr-ecoshard-data/ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "masked_nathab_esa.tif",
        },
        {
            'expression': 'mask(raster, %s, invert=False)'%(str([20,30]+[x for x in range(80,127)])[1:-1]),
            'symbol_to_path_map': {
                'raster': 'https://storage.googleapis.com/ecoshard-root/working-shards/coopernicus_landcover_discrete_compressed_md5_264bda5338a02e4a6cc10412b8edad9f.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "masked_nathab_copernicus.tif",
        },
        {
            # this is for masking out forest from natural habitat, for livestock production
            # this counts the >50% herbaceous / < 50% tree cover category as "not forest"; also includes lichens, mosses  and shrubland which maybe isn't totally edible by cattle either
            'expression': 'mask(raster, %s, invert=False)'%(str([x for x in range(110,154)]+[180])[1:-1]),
            'symbol_to_path_map': {
                'raster': 'https://storage.googleapis.com/ipbes-ndr-ecoshard-data/ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif',
            },
            'target_nodata': -1,
            'target_raster_path': "masked_nathab_notforest_esa.tif",
        },
    ]
    for masker in masker_list:
       raster_calculations_core.evaluate_calculation(
            masker, TASK_GRAPH, WORKSPACE_DIR)

     #dasgupta calcs: 

    raster_list = [
        {
            'expression': 'total_realized /total_potential',
            'symbol_to_path_map': {
                'total_realized': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4\prod_total_realized_en_10s_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
                'total_potential': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4\prod_total_potential_en_10s_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
            },
            'target_nodata': -1,
            'default_nan': -1, # this is necessary because divides by 0's; could also set them to 0 instead 
            'target_raster_path': "percent_realized_current.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'total_realized /total_potential',
            'symbol_to_path_map': {
                'total_realized': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_bau_esa_classes_md5_b411f14d7cff237e3415c5afa26d4b78\prod_total_realized_en_10s_lulc_WB_bau_esa_classes_md5_b411f14d7cff237e3415c5afa26d4b78.tif",
                'total_potential': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_bau_esa_classes_md5_b411f14d7cff237e3415c5afa26d4b78\prod_total_potential_en_10s_lulc_WB_bau_esa_classes_md5_b411f14d7cff237e3415c5afa26d4b78.tif",
            },
            'target_nodata': -1,
            'default_nan': -1,
            'target_raster_path': "percent_realized_bau.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'total_realized /total_potential',
            'symbol_to_path_map': {
                'total_realized': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_cons_esa_classes_md5_8c150474406a3f230b992399429bd182\prod_total_realized_en_10s_lulc_WB_cons_esa_classes_md5_8c150474406a3f230b992399429bd182.tif",
                'total_potential': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_cons_esa_classes_md5_8c150474406a3f230b992399429bd182\prod_total_potential_en_10s_lulc_WB_cons_esa_classes_md5_8c150474406a3f230b992399429bd182.tif",
            },
            'target_nodata': -1,
            'default_nan': -1,
            'target_raster_path': "percent_realized_cons.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'total_realized /total_potential',
            'symbol_to_path_map': {
                'total_realized': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_mid\prod_total_realized_en_10s_lulc_WB_mid.tif",
                'total_potential': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_mid\prod_total_potential_en_10s_lulc_WB_mid.tif",
            },
            'target_nodata': -1,
            'default_nan': -1,
            'target_raster_path': "percent_realized_mid.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'total_realized /total_potential',
            'symbol_to_path_map': {
                'total_realized': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4\prod_total_realized_va_10s_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
                'total_potential': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4\prod_total_potential_va_10s_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
            },
            'target_nodata': -1,
            'default_nan': -1,  
            'target_raster_path': "percent_realized_current_va.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'total_realized /total_potential',
            'symbol_to_path_map': {
                'total_realized': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_bau_esa_classes_md5_b411f14d7cff237e3415c5afa26d4b78\prod_total_realized_va_10s_lulc_WB_bau_esa_classes_md5_b411f14d7cff237e3415c5afa26d4b78.tif",
                'total_potential': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_bau_esa_classes_md5_b411f14d7cff237e3415c5afa26d4b78\prod_total_potential_va_10s_lulc_WB_bau_esa_classes_md5_b411f14d7cff237e3415c5afa26d4b78.tif",
            },
            'target_nodata': -1,
            'default_nan': -1,
            'target_raster_path': "percent_realized_bau_va.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'total_realized /total_potential',
            'symbol_to_path_map': {
                'total_realized': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_cons_esa_classes_md5_8c150474406a3f230b992399429bd182\prod_total_realized_va_10s_lulc_WB_cons_esa_classes_md5_8c150474406a3f230b992399429bd182.tif",
                'total_potential': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_cons_esa_classes_md5_8c150474406a3f230b992399429bd182\prod_total_potential_va_10s_lulc_WB_cons_esa_classes_md5_8c150474406a3f230b992399429bd182.tif",
            },
            'target_nodata': -1,
            'default_nan': -1,
            'target_raster_path': "percent_realized_cons_va.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
        {
            'expression': 'total_realized /total_potential',
            'symbol_to_path_map': {
                'total_realized': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_mid\prod_total_realized_va_10s_lulc_WB_mid.tif",
                'total_potential': r"C:\Users\Becky\Documents\dasgupta\nci_ag_multi_lulc\lulc_WB_mid\prod_total_potential_va_10s_lulc_WB_mid.tif",
            },
            'target_nodata': -1,
            'default_nan': -1,
            'target_raster_path': "percent_realized_mid_va.tif",
            'target_pixel_size': (0.002777777777778, -0.002777777777778),
        },
    ]

    for calculation in raster_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return   



if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(WORKSPACE_DIR, NCPUS, 5.0)
    main()
