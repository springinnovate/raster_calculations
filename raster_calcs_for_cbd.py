"""These calculations are just for fun."""
#cd C:\Users\Becky\Documents\raster_calculations
#conda activate py38_gdal312

import logging
import multiprocessing
import os
import sys

from ecoshard import geoprocessing
from ecoshard import taskgraph
from osgeo import gdal
from osgeo import osr
import numpy
import raster_calculations_core

gdal.SetCacheMax(2**26)

WORKSPACE_DIR = 'rastercalc_workspace'
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

    calculation_list = [
        {
            'expression': '(raster2*(raster2>=0)/(raster1/100))+((raster2<0)*-9999)',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\esa_pixel_area_ha_md5_1dd3298a7c4d25c891a11e01868b5db6.tif",
                'raster2': r"D:\ecoshard\CBD_GBF_IIS\global_n_export_lulc_sc1_fertilizer_2050_compressed_md5_c215e5207337873a4c527b173c0d9ae2.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "global_n_export_lulc_sc1_fertilizer_2050_persqkm.tif",
        },
        {
            'expression': 'raster2/(raster1/100)',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\esa_pixel_area_ha_md5_1dd3298a7c4d25c891a11e01868b5db6.tif",
                'raster2': r"D:\ecoshard\CBD_GBF_IIS\ESA_2015_mod_IIS_cv_habitat_value.tif",
            },
            'target_nodata': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "ESA_2015_mod_IIS_cv_habitat_value_persqkm.tif",
        },
        #{
        #    'expression': '(raster2*(raster2<10000)/(raster1/100))+((raster2>=10000)*-9999)',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\ecoshard\esa_pixel_area_ha_md5_1dd3298a7c4d25c891a11e01868b5db6.tif",
        #        'raster2': r"D:\ecoshard\CBD_GBF_IIS\IIS_avoided_conversion_nitrogen_Sc2-Sc1_fert2050_md5_5432ac_compressed.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
        #    'resample_method': 'near',
        #    'target_raster_path': "IIS_avoided_conversion_nitrogen_persqkm_Sc2-Sc1_fert2050.tif",
        #},
        #{
        #    'expression': '(raster2*(raster2<10000)/(raster1/100))+((raster2>=10000)*-9999)',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\ecoshard\esa_pixel_area_ha_md5_1dd3298a7c4d25c891a11e01868b5db6.tif",
        #        'raster2': r"D:\ecoshard\CBD_GBF_IIS\IIS_restoration_nitrogen_Sc1-Sc3_fert2050_md5_4da71d_compressed.tif",
        #    },
        #    'target_nodata': -9999,
        #    'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
        #    'resample_method': 'near',
        #    'target_raster_path': "IIS_restoration_nitrogen_persqkm_Sc1-Sc3_fert2050.tif",
        #},
        #{
        #    'expression': '(raster2)/(raster1/100)',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\ecoshard\esa_pixel_area_ha_md5_1dd3298a7c4d25c891a11e01868b5db6.tif",
        #        'raster2': r"D:\ecoshard\CBD_GBF_IIS\IIS_avoided_conversion_CRR_Sc1-Sc2_md5_198bf3_compressed.tif",
        #    },
        #    'target_nodata': -9999,
        #     'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
        #    'resample_method': 'near',
        #    'target_raster_path': "IIS_avoided_conversion_CRR_persqkm_Sc1-Sc2",
        #},
        #{
        #    'expression': '(raster2)/(raster1/100)',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\ecoshard\esa_pixel_area_ha_md5_1dd3298a7c4d25c891a11e01868b5db6.tif",
        #        'raster2': r"D:\ecoshard\CBD_GBF_IIS\IIS_restoration_CRR_Sc3-Sc1_md5_46e98f_compressed.tif",
        #    },
        #    'target_nodata': -9999,
        #     'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
        #    'resample_method': 'near',
        #    'target_raster_path': "IIS_restoration_CRR_persqkm_Sc3-Sc1.tif",
        #},
        #{
        #    'expression': '(raster2)/(raster1/100)',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\ecoshard\esa_pixel_area_ha_md5_1dd3298a7c4d25c891a11e01868b5db6.tif",
        #        'raster2': r"D:\ecoshard\CBD_GBF_IIS\cv_pop_on_habitat_PNV_md5_2e35bd.tif",
        #    },
        #    'target_nodata': -9999,
        #     'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
        #    'resample_method': 'near',
        #    'target_raster_path': "IIS_cv_pop_persqkm.tif",
        #},
        #{
        #    'expression': '(raster2)/(raster1/100)',
        #    'symbol_to_path_map': {
        #        'raster1': r"D:\ecoshard\esa_pixel_area_ha_md5_1dd3298a7c4d25c891a11e01868b5db6.tif",
        #        'raster2': r"D:\repositories\ecoshard\ipbesdata\downstream_bene_2017_500000.tif",
        #    },
        #    'target_nodata': -9999,
        #     'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
        #    'resample_method': 'near',
        #    'target_raster_path': "IIS_downstream_pop_persqkm.tif",
        #},
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': '(raster2>=raster1)*raster1 + (raster1>raster2)*raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CBD_GBF_IIS\esa_realized_pollination_per_ha_md5_a6a7ffd6629701c0145166ad88b7d7dd.tif",
                'raster2': r"D:\ecoshard\CBD_GBF_IIS\pnv_realized_pollination_per_ha_md5_88408670a144e45c32e03e92f8edd8fb.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "avoided_conversion_pollination_per_ha_esa2018.tif",
        },
        {
            'expression': '(raster2>raster1)*(raster2-raster1)',
            'symbol_to_path_map': {
                'raster1': r"D:\ecoshard\CBD_GBF_IIS\esa_realized_pollination_per_ha_md5_a6a7ffd6629701c0145166ad88b7d7dd.tif",
                'raster2': r"D:\ecoshard\CBD_GBF_IIS\pnv_realized_pollination_per_ha_md5_88408670a144e45c32e03e92f8edd8fb.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "restoration_pollination_per_ha_Sc3-esa2018.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': '(raster2>=raster1)*raster1 + (raster1>raster2)*raster2',
            'symbol_to_path_map': {
                'raster1': r"D:\results\ESA_realized_pollination_10s_esa2018_md5_58acac8dc45739943b21b4fd612829d7_ovr.tif",
                'raster2': r"D:\results\PNV_realized_pollination_ovr_compressed_md5_905ca9a17ae9e236f0f548538ba7c9a0.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "avoided_conversion_pollination_esa2018.tif",
        },
        {
            'expression': '(raster2>raster1)*(raster2-raster1)',
            'symbol_to_path_map': {
                'raster1': r"D:\results\ESA_realized_pollination_10s_esa2018_md5_58acac8dc45739943b21b4fd612829d7_ovr.tif",
                'raster2': r"D:\results\PNV_realized_pollination_ovr_compressed_md5_905ca9a17ae9e236f0f548538ba7c9a0.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "restoration_pollination_Sc3-esa2018.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


    calculation_list = [
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\raster_calculations\avoided_conversion_nitrogen_Sc2-Sc1_fert2050_compressed_md5_51c49f050fc15a2f0ebe06b57ceae1b4.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "IIS_avoided_conversion_nitrogen_Sc2-Sc1_fert2050.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\raster_calculations\restoration_nitrogen_Sc1-Sc3_fert2050_compressed_md5_275ccc46bab1fce1876c81abe396687a.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "IIS_restoration_nitrogen_Sc1-Sc3_fert2050.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\raster_calculations\restoration_CRR_Sc3-Sc1.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "IIS_restoration_CRR_Sc3-Sc1.tif",
        },
        {
            'expression': '(raster1>0)*raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\repositories\raster_calculations\avoided_conversion_CRR_Sc1-Sc2.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "IIS_avoided_conversion_CRR_Sc1-Sc2.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    calculation_list = [
        {
            'expression': 'raster2 - raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\results\global_n_export_lulc_sc1_fertilizer_2050_compressed_md5_c215e5207337873a4c527b173c0d9ae2.tif",
                'raster2': r"D:\results\global_n_export_lulc_sc2_fertilizer_2050_compressed_md5_6a5b8f74d205e7ba8614e8ca70bce00f.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "avoided_conversion_nitrogen_Sc2-Sc1_fert2050.tif",
        },
        {
            'expression': 'raster2 - raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\results\global_n_export_lulc_sc1_fertilizer_intensified_compressed_md5_2dfa9baf7db8e015fea34c61e4e90b7b.tif",
                'raster2': r"D:\results\global_n_export_lulc_sc2_fertilizer_intensified_compressed_md5_cfd2a69156533f340f044cbc11d89856.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "avoided_conversion_nitrogen_Sc2-Sc1_fertInt.tif",
        },
        {
            'expression': 'raster2 - raster1',
            'symbol_to_path_map': {
                'raster1': r"D:\results\global_n_export_lulc_sc1_fertilizer_current_compressed_md5_5cdf0610c72b09215bcf686b30d6edfd.tif",
                'raster2': r"D:\results\global_n_export_lulc_sc2_fertilizer_current_compressed_md5_aa1730466d71ee95c89f8a2c6cdfb312.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "avoided_conversion_nitrogen_Sc2-Sc1_fertCur.tif",
        },
        {
            'expression': 'raster1 - raster3',
            'symbol_to_path_map': {
                'raster1': r"D:\results\global_n_export_lulc_sc1_fertilizer_2050_compressed_md5_c215e5207337873a4c527b173c0d9ae2.tif",
                'raster3': r"D:\results\global_n_export_lulc_sc3_fertilizer_current_compressed_md5_95bcca83bdbecdeaa958d0a954df1794.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "restoration_nitrogen_Sc1-Sc3_fert2050.tif",
        },
        {
            'expression': 'raster1 - raster3',
            'symbol_to_path_map': {
                'raster1': r"D:\results\global_n_export_lulc_sc1_fertilizer_intensified_compressed_md5_2dfa9baf7db8e015fea34c61e4e90b7b.tif",
                'raster3': r"D:\results\global_n_export_lulc_sc3_fertilizer_current_compressed_md5_95bcca83bdbecdeaa958d0a954df1794.tif",
            },
            'target_nodata': -1e34,
            'target_raster_path': "restoration_nitrogen_Sc1-Sc3_fertInt.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


#for new "current" scenario::
    # forest = 80; natural grasslands = 150; wetlands = 180; deserts = 200; shrublands = 120; cultivated grasslands = 50; croplands = 20.
    # With that map, you are going to reclass the "bare lands" in your esa map (which is in 300m resolution).
    # When you encounter shrublands, grasslands, deserts and bare lands in your current esas map, and ours is "cultivated grasslands" you will change yours.
    # You will also change the "desert" class to "sparse".


    calculation_list = [
        {
            'expression': 'raster1 + raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\ci-global-restoration\Nbackgroundrates_smithpnv_md5_70ffbb628551efdf7b086de8258681fc.tif",
                'raster2': r"C:\Users\Becky\Documents\NfertratescurrentRevQ_md5_092d4fd6658df3073fd0c8d9c5a974cf.tif",
            },
            'target_nodata': -9999,
            'default_nan': -9999,
            'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
            'resample_method': 'near',
            'target_raster_path': "Nrates_NCIcurrentRevQ_add_smithpnv_background.tif",
        },
    ]

    for calculation in calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    single_expression = {
        'expression': (
            '(raster1>=200)*(raster1<=202)*(raster2>79)*(raster2<81)*50 + (raster1>=200)*(raster1<=202)*(raster2>149)*(raster2<151)*130 +'
            '(raster1>=200)*(raster1<=202)*(raster2>179)*(raster2<181)*180 + (raster1>=200)*(raster1<=202)*(raster2>119)*(raster2<121)*120 +'
            '(raster1>=200)*(raster1<=202)*(raster2>49)*(raster2<51)*30 + (raster1>=200)*(raster1<=202)*(raster2>19)*(raster2<21)*10 +'
            '(raster1>=200)*(raster1<=202)*(raster2>199)*(raster2<201)*150 + (raster1>=100)*(raster1<=153)*(raster2>49)*(raster2<51)*30 +'
            '(raster1>=100)*(raster1<=153)*(raster2<50)*raster1 + (raster1>=100)*(raster1<=153)*(raster2>50)*raster1 +'
            '(raster1<100)*raster1 + (raster1>153)*(raster1<200)*raster1 + (raster1>202)*raster1'
            ),
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Documents\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
            'raster2': r"C:\Users\Becky\Documents\cbd\align_to_mask_workspace\CurrentLULC_ESAclasses_max_WARPED_max_md5_0a62fae39f2c5990cd669f18c148e4ea.tif",
            #'raster3': r"C:\Users\Becky\Documents\ci-global-restoration\PNV_all_ecosystems\PNV_smith_060420_md5_8dd464e0e23fefaaabe52e44aa296330.tif"
        },
        'target_nodata': 0,
        'target_pixel_size': (0.0027777777777777778,-0.0027777777777777778),
        'resample_method': 'near',
        'target_raster_path': "ESA_2015_mod_IIS.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    raster_calculation_list = [
        {
            'expression': '(raster1 - raster2)*(raster1<2)',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cbd\Pollination\PNV\ESACCI_PNV_iis_OA_ESAclasses_max_ESAresproj_md5_e6575db589abb52c683d44434d428d80_hab_mask.tif",
                'raster2': r"C:\Users\Becky\Documents\cbd\Pollination\PNV\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2018-v2.1.1_hab_mask_md5_9afb78a2cc68a7bf6bba947761d74fc3.tif",
            },
            'target_nodata': -9999,
            'target_raster_path': "restored_iis-esa.tif",
        },
    ]

    for calculation in raster_calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

###################PRE-PROCESSING
#their scenario maps are weird.
# here's a snippet that will reproject it to the esa bounding box and size:
    esa_info = geoprocessing.get_raster_info("ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif")
    base_raster_path = r"ESACCI_PNV_iis_OA_ESAclasses_max_md5_e6575db589abb52c683d44434d428d80.tif"
    target_raster_path = '%s_wgs84%s' % os.path.splitext(base_raster_path)
    geoprocessing.warp_raster(
        base_raster_path, esa_info['pixel_size'], target_raster_path,
        'near', target_projection_wkt=esa_info['projection_wkt'],
        target_bb=esa_info['bounding_box'])

    return

#that was fine but the nodata was screwed up and then I couldn't reset it. So did it this way instead:
    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)

    raster_calculation_list = [
        {
            'expression': '(raster2>0)*raster1',
            'symbol_to_path_map': {
                'raster1': "ESACCI_PNV_iis_OA_ESAclasses_max_md5_e6575db589abb52c683d44434d428d80.tif",
                'raster2':"ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif",
            },
            'target_nodata': 0,
            'target_projection_wkt': wgs84_srs.ExportToWkt(),
            'target_pixel_size': (0.002777777777778,0.002777777777778),
            'bounding_box_mode': [-180, -90, 180, 90],
            'resample_method': 'near',
            'target_raster_path': "ESACCI_PNV_iis_OA_ESAclasses_max_ESAresproj_md5_e6575db589abb52c683d44434d428d80.tif",
        },
    ]

    for calculation in raster_calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    # Adjusting SSP3 pop increase to LSPOP baseline

    single_expression = {
        'expression': '(raster1/raster2)*raster3*(raster2>0) + (raster2==0)*raster3', #I don't know why this produces negative nodata values
        'symbol_to_path_map': {
            'raster1': r"C:\Users\Becky\Downloads\ssp3_2050_md5_b0608d53870b9a7e315bf9593c43be86.tif",
            'raster2': r"C:\Users\Becky\Downloads\ssp1_2010_md5_5edda6266351ccc7dbd587c89fa2ab65.tif",
            'raster3': r"C:\Users\Becky\Documents\raster_calculations\lspop2017.tif",
        },
        'target_nodata': 2147483647,
        'default_nan': 2147483647,
        'target_pixel_size': (0.002777777777778,0.002777777777778),
        'resample_method': 'near',
        'target_raster_path': "lspop_ssp3.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()

    single_expression = {
        'expression': '(raster1>=0)*raster1',
        'symbol_to_path_map': {
            'raster1': "lspop_ssp3.tif",
        },
        'target_nodata': 2147483647,
        'default_nan': 2147483647,
        'target_raster_path': "lspop_ssp3_noneg.tif",
    }

    raster_calculations_core.evaluate_calculation(
        single_expression, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


###I tried to incorporate future sea level rise but it was way coarser, and way less spatially variable, so I gave up.
    # mean value of ssp3: 0.49802045465938
    # mean value of current: 2.9183045296182

    raster_calculation_list = [
        {
            'expression': '(raster1/2.9183045296182)',
            'symbol_to_path_map': {
                'raster1': "MSL_Map_MERGED_Global_AVISO_NoGIA_Adjust_md5_3072845759841d0b2523d00fe9518fee.tif",
            },
            'target_nodata': float(numpy.finfo(numpy.float32).min),
            'target_raster_path': "MSL_Map_MERGED_Global_AVISO_NoGIA_Adjust_meannorm.tif",
        },
        {
            'expression': '(raster1/0.49802045465938)',
            'symbol_to_path_map': {
                'raster1': "slr_rcp60_md5_99ccaf1319d665b107a9227f2bbbd8b6_wgs84.tif",
            },
            'target_nodata': float(numpy.finfo(numpy.float32).min),
            'target_raster_path': "slr_rcp60_wgs84_meannorm.tif",
        },
    ]

    for calculation in raster_calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    esa_info = geoprocessing.get_raster_info("MSL_Map_MERGED_Global_AVISO_NoGIA_Adjust_md5_3072845759841d0b2523d00fe9518fee.tif")
    base_raster_path = r"slr_rcp60_md5_99ccaf1319d665b107a9227f2bbbd8b6.tif"
    target_raster_path = '%s_wgs84%s' % os.path.splitext(base_raster_path)
    geoprocessing.warp_raster(
        base_raster_path, esa_info['pixel_size'], target_raster_path,
        'near', target_projection_wkt=esa_info['projection_wkt'],
        target_bb=esa_info['bounding_box'])

    return

##### in sum, annoying layer to deal with, didn't use it.


############################### POST PROCESSING
    raster_calculation_list = [
        {
            'expression': 'raster1/raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cbd\Pollination\ESA_realized_pollination_10s_esa2018_md5_58acac8dc45739943b21b4fd612829d7_ovr.tif",
                'raster2': r"C:\Users\Becky\Documents\esa_pixel_area_ha.tif",
            },
            'target_nodata': float(numpy.finfo(numpy.float32).min),
            'target_pixel_size': (0.002777777777778,0.002777777777778),
            'resample_method': 'near',
            'target_raster_path': "esa2015_realized_pollination_per_ha.tif",
        },
        {
            'expression': 'raster1/raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cbd\Pollination\PNV_realized_pollination_with_overviews.tif",
                'raster2': r"C:\Users\Becky\Documents\esa_pixel_area_ha.tif",
            },
            'target_nodata': float(numpy.finfo(numpy.float32).min),
            'target_pixel_size': (0.002777777777778,0.002777777777778),
            'resample_method': 'near',
            'target_raster_path': "pnv_realized_pollination_per_ha.tif",
        },
    ]

    for calculation in raster_calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    raster_calculation_list = [
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cbd\ndr\compressed_esa2015_driverssp3_300.0_D8_modified_load.tif",
                'raster2':r"C:\Users\Becky\Documents\cbd\ndr\compressed_esa2015_driverssp3_300.0_D8_export.tif",
            },
            'target_nodata': float(numpy.finfo(numpy.float32).min),
            'target_raster_path': "esa2015_driverssp3_300.0_D8_n_retention.tif",
        },
        {
            'expression': 'raster1 - raster2',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cbd\ndr\compressed_pnv_driverssp3_300.0_D8_modified_load.tif",
                'raster2':r"C:\Users\Becky\Documents\cbd\ndr\compressed_pnv_driverssp3_300.0_D8_export.tif",
            },
            'target_nodata': float(numpy.finfo(numpy.float32).min),
            'target_raster_path': "pnv_driverssp3_300.0_D8_n_retention.tif",
        },
    ]

    for calculation in raster_calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()

    raster_calculation_list = [
        {
            'expression': '(raster1/raster2)',
            'symbol_to_path_map': {
                'raster1': "esa2015_driverssp3_300.0_D8_n_retention.tif",
                'raster2': r"C:\Users\Becky\Documents\esa_pixel_area_ha.tif",
            },
            'target_nodata': float(numpy.finfo(numpy.float32).min),
            'target_raster_path': "esa2015_driverssp3_300.0_D8_n_retention_per_ha.tif",
        },
        {
            'expression': '(raster1/raster2)',
            'symbol_to_path_map': {
                'raster1': "pnv_driverssp3_300.0_D8_n_retention.tif",
                'raster2': r"C:\Users\Becky\Documents\esa_pixel_area_ha.tif",
            },
            'target_nodata': float(numpy.finfo(numpy.float32).min),
            'target_raster_path': "pnv_driverssp3_300.0_D8_n_retention_per_ha.tif",
        },
    ]

    for calculation in raster_calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return

    raster_calculation_list = [
        {
            'expression': 'raster1*raster2/raster3',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cbd\CV\cv_pop_esa2015_ssp3.tif",
                'raster2':r"C:\Users\Becky\Documents\cbd\CV\cv_value_esa2015_ssp3.tif",
                'raster3': r"C:\Users\Becky\Documents\esa_pixel_area_ha.tif",
            },
            'target_nodata': float(numpy.finfo(numpy.float32).min),
            'target_pixel_size': (0.002777777777778,0.002777777777778),
            'resample_method': 'near',
            'target_raster_path': "esa2015_ssp3_realized_coastal_risk_reduction_per_ha.tif",
        },
        {
            'expression': 'raster1*raster2/raster3',
            'symbol_to_path_map': {
                'raster1': r"C:\Users\Becky\Documents\cbd\CV\cv_pop_pnv_ssp3.tif",
                'raster2':r"C:\Users\Becky\Documents\cbd\CV\cv_value_pnv_ssp3.tif",
                'raster3': r"C:\Users\Becky\Documents\esa_pixel_area_ha.tif",
            },
            'target_nodata': float(numpy.finfo(numpy.float32).min),
            'target_pixel_size': (0.002777777777778,0.002777777777778),
            'resample_method': 'near',
            'target_raster_path': "pnv_ssp3_realized_coastal_risk_reduction_per_ha.tif",
        },
    ]

    for calculation in raster_calculation_list:
        raster_calculations_core.evaluate_calculation(
            calculation, TASK_GRAPH, WORKSPACE_DIR)

    TASK_GRAPH.join()
    TASK_GRAPH.close()

    return


if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(WORKSPACE_DIR, NCPUS, 5.0)
    main()
