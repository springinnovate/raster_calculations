"""
Ag analysis for NCI project. This is based off the IPBES-Pollination
project so that Peter can run specific landcover maps with given price data.
"""
import argparse
import collections
import glob
import itertools
import logging
import multiprocessing
import os
import re
import sys
import time
import zipfile

from osgeo import gdal
from osgeo import ogr
from osgeo import osr
import ecoshard
import numpy
import pandas
import pygeoprocessing
import rtree
import scipy.ndimage.morphology
import shapely.prepared
import shapely.wkb
import taskgraph

# format of the key pairs is [data suffix]: [landcover raster]
# these must be ESA landcover map type
LANDCOVER_DATA_MAP = {
    'data_suffix': 'landcover raster.tif',
}

# set a 1GB limit for the cache
gdal.SetCacheMax(2**30)

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger('nci_pollination')
logging.getLogger('taskgraph').setLevel(logging.INFO)

_MULT_NODATA = -1
_MASK_NODATA = 2
# the following are the globio landcover codes. A tuple (x, y) indicates the
# inclusive range from x to y. Pollinator habitat was defined as any natural
# land covers, as defined (GLOBIO land-cover classes 6, secondary vegetation,
# and  50-180, various types of primary vegetation). To test sensitivity to
# this definition we included "semi-natural" habitats (GLOBIO land-cover
# classes 3, 4, and 5; pasture, rangeland and forestry, respectively) in
# addition to "natural", and repeated all analyses with semi-natural  plus
# natural habitats, but this did not substantially alter the results  so we do
# not include it in our final analysis or code base.

GLOBIO_AG_CODES = [2, (10, 40), (230, 232)]
GLOBIO_NATURAL_CODES = [6, (50, 180)]
BMP_LULC_CODES = [300]

WORKING_DIR = './nci_ag_workspace'
ECOSHARD_DIR = os.path.join(WORKING_DIR, 'ecoshard_dir')
CHURN_DIR = os.path.join(WORKING_DIR, 'churn')

NODATA = -9999
N_WORKERS = max(1, multiprocessing.cpu_count())

COUNTRY_ISO_GPKG_URL = (
    'https://storage.googleapis.com/nci-ecoshards/'
    'country_shapefile-20191004T192454Z-001_'
    'md5_4eb621c6c74707f038d9ac86a4f2b662.gpkg')
YIELD_AND_HAREA_ZIP_URL = (
    'https://storage.googleapis.com/nci-ecoshards/'
    'ipbes_monfreda_2008_observed_yield_and_harea_'
    'md5_49b529f57071cc85abbd02b6e105089b.zip')
AG_COST_TABLE_URL = (
    'https://storage.googleapis.com/nci-ecoshards/'
    'ag_cost_md5_872f0d09f6d7add60c733cccc3b26987.csv')
COUNTRY_REGION_ISO_TABLE_URL = (
    'https://storage.googleapis.com/nci-ecoshards/'
    'country_region_iso_table_md5_a92f42ebb33d9f3fb198cc4909499cdd.csv')
PRICES_BY_CROP_AND_COUNTRY_TABLE_URL = (
    'https://storage.googleapis.com/nci-ecoshards/'
    'prices_by_crop_and_country_md5_a1fc8160fac6fa4f34844ab29c92c38a.csv')
CROP_NUTRIENT_URL = (
    'https://storage.googleapis.com/nci-ecoshards/'
    'crop_nutrient_md5_2fbe7455357f8008a12827fd88816fc1.csv')
FERT_USAGE_RASTERS_URL = (
    'https://storage.googleapis.com/nci-ecoshards/'
    'Fertilizer2000toMarijn_geotiff_md5_2bcd8efada8228f2b90e1491abf96645.zip')
CBI_MOD_YIELD_TABLES_URL = (
    'https://storage.googleapis.com/nci-ecoshards/'
    'cbi_mod_yield_md5_c682def6487414656d4dc76675f2920a.zip')
EXTENDED_CLIMATE_BIN_RASTERS_URL = (
    'https://storage.googleapis.com/nci-ecoshards/'
    'extended_climate_bin_maps_for_11_crops_'
    'md5_a44d69867c39a109e21627bb71d118cc.zip')

ADJUSTED_GLOBAL_PRICE_TABLE_PATH = os.path.join(
    CHURN_DIR, 'adjusted_global_price_map.csv')
AVG_GLOBAL_LABOR_COST_TABLE_PATH = os.path.join(
    CHURN_DIR, 'global_labor_cost.csv')
AVG_GLOBAL_MACH_COST_TABLE_PATH = os.path.join(
    CHURN_DIR, 'global_mach_cost.csv')
AVG_GLOBAL_N_COST_TABLE_PATH = os.path.join(
    CHURN_DIR, 'global_N_cost.csv')
AVG_GLOBAL_P_COST_TABLE_PATH = os.path.join(
    CHURN_DIR, 'global_P_cost.csv')
AVG_GLOBAL_K_COST_TABLE_PATH = os.path.join(
    CHURN_DIR, 'global_K_cost.csv')
PER_COUNTRY_PRICE_SCALE_FACTOR_TABLE_PATH = os.path.join(
    CHURN_DIR, 'per_country_price_scale_factor.csv')
COUNTRY_CROP_PRICE_TABLE_PATH = os.path.join(
    CHURN_DIR, 'country_crop_price_table.csv')
CBI_MOD_YIELD_TABLES_DIR = os.path.join(
    ECOSHARD_DIR, 'cbi_mod_yield_tables')
CBI_MOD_YIELD_RASTERS_DIR = os.path.join(
    CHURN_DIR, 'cbi_mod_yield_rasters')
COUNTRY_REGION_ISO_TABLE_PATH = os.path.join(
    ECOSHARD_DIR, os.path.basename(COUNTRY_REGION_ISO_TABLE_URL))
COUNTRY_ISO_GPKG_PATH = os.path.join(
    ECOSHARD_DIR, os.path.basename(COUNTRY_ISO_GPKG_URL))
CROP_NUTRIENT_TABLE_PATH = os.path.join(
    ECOSHARD_DIR, os.path.basename(CROP_NUTRIENT_URL))
PRICES_BY_CROP_AND_COUNTRY_TABLE_PATH = os.path.join(
    ECOSHARD_DIR, os.path.basename(PRICES_BY_CROP_AND_COUNTRY_TABLE_URL))
AG_COSTS_TABLE_PATH = os.path.join(
    ECOSHARD_DIR, os.path.basename(AG_COST_TABLE_URL))
YIELD_AND_HAREA_RASTER_DIR = os.path.join(
    ECOSHARD_DIR, 'monfreda_2008_observed_yield_and_harea')
FERT_APP_RATE_DIR = os.path.join(
    ECOSHARD_DIR, 'Fertilizer2000toMarijn_geotiff')
CROP_PRICE_DIR = os.path.join(CHURN_DIR, 'crop_prices')
CROP_COSTS_DIR = os.path.join(CHURN_DIR, 'crop_costs')
CROP_COSTS_WORKING_DIR = os.path.join(CROP_COSTS_DIR, 'per_element_costs')
FERT_USAGE_DIR = os.path.join(
    ECOSHARD_DIR, 'Fertilizer2000toMarijn_geotiff')


def calculate_for_landcover(task_graph, landcover_path, valid_crop_set):
    """Calculate values for a given landcover.
    Parameters:
        task_graph (taskgraph.TaskGraph): taskgraph object used to schedule
            work.
        landcover_path (str): path to a landcover map with globio style
            landcover codes.
        valid_crop_set (set): set of Monfreda style crop ids that are the only
            crops that should be processed.
    Returns:
        None.
    """
    landcover_key = os.path.splitext(os.path.basename(landcover_path))[0]
    output_dir = os.path.join(WORKING_DIR, landcover_key)
    for dir_path in [output_dir, ECOSHARD_DIR, CHURN_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass
    # Crop content of critical macro and micronutrients (KJ energy/100 g, IU
    #   Vitamin A/ 100 g and mcg Folate/100g) for the 115 crops were taken
    #   from USDA (2011) . The USDA (2011) data also provided estimated refuse
    #   of the food item (e.g., peels, seeds). The pollination-dependent yield
    #   was reduced by this refuse percent and then multiplied by nutrient
    #   content, and summed across all crops to derive pollination-dependent
    #   nutrient yields (KJ/ha, IU Vitamin A/ha, mcg Folate/ha) for each
    #   nutrient at 5 arc min. The full table used in this analysis can be
    # found at https://storage.googleapis.com/ecoshard-root/'
    # 'crop_nutrient_md5_d6e67fd79ef95ab2dd44ca3432e9bb4d.csv
    target_10km_value_yield_path = os.path.join(
        CHURN_DIR, 'monfreda_2008_value_yield_rasters',
        'monfreda_2008_value_yield_total_10km.tif')
    target_10s_value_yield_path = os.path.join(
        output_dir,
        'monfreda_2008_value_yield_total_10s_%s.tif' % landcover_key)
    target_10s_value_path = os.path.join(
        output_dir,
        'monfreda_2008_value_total_10s_%s.tif' % landcover_key)

    target_10km_cost_yield_path = os.path.join(
        CHURN_DIR, 'monfreda_2008_cost_yield_rasters',
        'monfreda_2008_cost_yield_total_10km.tif')
    target_10s_cost_yield_path = os.path.join(
        output_dir,
        'monfreda_2008_cost_yield_total_10s_%s.tif' % landcover_key)
    target_10s_cost_path = os.path.join(
        output_dir,
        'monfreda_2008_cost_total_10s_%s.tif' % landcover_key)

    target_10km_profit_yield_path = os.path.join(
        CHURN_DIR, 'monfreda_2008_profit_yield_rasters',
        'monfreda_2008_profit_yield_total_10km.tif')
    target_10s_profit_yield_path = os.path.join(
        output_dir,
        'monfreda_2008_profit_yield_total_10s_%s.tif' % landcover_key)
    target_10s_profit_path = os.path.join(
        output_dir,
        'monfreda_2008_profit_total_10s_%s.tif' % landcover_key)

    target_10s_harea_path = os.path.join(
        output_dir,
        'monfreda_2008_harea_total_%s.tif' % landcover_key)

    create_value_rasters(
        task_graph,
        valid_crop_set,
        CROP_NUTRIENT_TABLE_PATH,
        # the `False` indicates "do not consider pollination"
        YIELD_AND_HAREA_RASTER_DIR, CROP_PRICE_DIR, CROP_COSTS_DIR,
        False, landcover_path, target_10km_value_yield_path,
        target_10s_value_yield_path, target_10s_value_path,
        target_10km_cost_yield_path,
        target_10s_cost_yield_path,
        target_10s_cost_path,
        target_10km_profit_yield_path,
        target_10s_profit_yield_path,
        target_10s_profit_path,
        target_10s_harea_path)

    # do poll dep value
    target_10km_poll_dep_value_yield_path = os.path.join(
        CHURN_DIR, 'monfreda_2008_poll_dep_value_yield_rasters',
        'monfreda_2008_poll_dep_value_yield_total_10km_%s.tif' %
        landcover_key)
    target_10s_poll_dep_value_yield_path = os.path.join(
        output_dir,
        'monfreda_2008_poll_dep_value_yield_total_10s_%s.tif' % landcover_key)
    target_10s_poll_dep_value_path = os.path.join(
        output_dir,
        'monfreda_2008_poll_dep_value_total_10s_%s.tif' % landcover_key)

    target_10km_poll_dep_cost_yield_path = os.path.join(
        CHURN_DIR, 'monfreda_2008_poll_dep_cost_yield_rasters',
        'monfreda_2008_poll_dep_cost_yield_total_10km_%s.tif' %
        landcover_key)
    target_10s_poll_dep_cost_yield_path = os.path.join(
        output_dir,
        'monfreda_2008_poll_dep_cost_yield_total_10s_%s.tif' % landcover_key)
    target_10s_poll_dep_cost_path = os.path.join(
        output_dir,
        'monfreda_2008_poll_dep_cost_total_10s_%s.tif' % landcover_key)

    target_10km_pol_dep_profit_yield_path = os.path.join(
        CHURN_DIR, 'monfreda_2008_pol_dep_profit_yield_rasters',
        'monfreda_2008_pol_dep_profit_yield_total_10km_%s.tif' % landcover_key)
    target_10s_pol_dep_profit_yield_path = os.path.join(
        output_dir,
        'monfreda_2008_pol_dep_profit_yield_total_10s_%s.tif' % landcover_key)
    target_10s_pol_dep_profit_path = os.path.join(
        output_dir,
        'monfreda_2008_pol_dep_profit_total_10s_%s.tif' % landcover_key)

    target_10s_pol_harea_path = os.path.join(
        CHURN_DIR, 'monfreda_2008_harea_total_rasters',
        'monfreda_2008_pol_harea_total_%s.tif' % landcover_key)

    create_value_rasters(
        task_graph,
        valid_crop_set,
        CROP_NUTRIENT_TABLE_PATH,
        # the `True` indicates "consider pollination"
        YIELD_AND_HAREA_RASTER_DIR, CROP_PRICE_DIR, CROP_COSTS_DIR, True,
        landcover_path,
        target_10km_poll_dep_value_yield_path,
        target_10s_poll_dep_value_yield_path,
        target_10s_poll_dep_value_path,
        target_10km_poll_dep_cost_yield_path,
        target_10s_poll_dep_cost_yield_path,
        target_10s_poll_dep_cost_path,
        target_10km_pol_dep_profit_yield_path,
        target_10s_pol_dep_profit_yield_path,
        target_10s_pol_dep_profit_path,
        target_10s_pol_harea_path)

    # 1.2.3.  Crop production

    # Spatially-explicit global crop yields (tons/ha) at 5 arc min (~10 km)
    # were taken from Monfreda et al. (2008) for 115 crops (permanent link to
    # crop yield folder). These yields were multiplied by crop pollination
    # dependency to calculate the pollination-dependent crop yield for each 5
    # min grid cell. Note the monfreda maps are in units of per-hectare
    # yields
    prod_total_nut_10s_task_path_map = {}
    poll_dep_prod_nut_10s_task_path_map = {}
    for nut_id, nutrient_name in [
            ('en', 'Energy'), ('va', 'VitA'), ('fo', 'Folate')]:
        # total annual production of nutrient
        yield_total_nut_10km_path = os.path.join(
            CHURN_DIR, 'monfreda_2008_yield_nutrient_rasters',
            f'monfreda_2008_yield_total_{nut_id}_10km.tif')
        yield_total_nut_10s_path = os.path.join(
            output_dir,
            f'monfreda_2008_yield_total_{nut_id}_10s_%s.tif' % landcover_key)
        prod_total_nut_10s_path = os.path.join(
            output_dir,
            f'monfreda_2008_prod_total_{nut_id}_10s_%s.tif' % landcover_key)

        prod_total_task = task_graph.add_task(
            func=create_prod_nutrient_raster,
            args=(
                 valid_crop_set,
                 CROP_NUTRIENT_TABLE_PATH, nutrient_name,
                 YIELD_AND_HAREA_RASTER_DIR, False, landcover_path,
                 yield_total_nut_10km_path, yield_total_nut_10s_path,
                 prod_total_nut_10s_path),
            target_path_list=[
                yield_total_nut_10km_path, yield_total_nut_10s_path,
                prod_total_nut_10s_path],
            task_name=f"""create prod raster {
                os.path.basename(prod_total_nut_10s_path)}""")
        prod_total_nut_10s_task_path_map[nut_id] = (
            prod_total_task, prod_total_nut_10s_path)

        # pollination-dependent annual production of nutrient
        poll_dep_yield_nut_10km_path = os.path.join(
            CHURN_DIR, 'monfreda_2008_yield_poll_dep_rasters',
            f'monfreda_2008_yield_poll_dep_{nut_id}_10km.tif')
        poll_dep_yield_nut_10s_path = os.path.join(
            CHURN_DIR, 'monfreda_2008_yield_poll_dep_rasters',
            f'monfreda_2008_yield_poll_dep_{nut_id}_10s_%s.tif' %
            landcover_key)
        poll_dep_prod_nut_10s_path = os.path.join(
            CHURN_DIR, 'monfreda_2008_prod_poll_dep_rasters',
            f'monfreda_2008_prod_poll_dep_{nut_id}_10s_%s.tif' % landcover_key)
        pol_dep_prod_task = task_graph.add_task(
            func=create_prod_nutrient_raster,
            args=(
                valid_crop_set,
                CROP_NUTRIENT_TABLE_PATH, nutrient_name,
                YIELD_AND_HAREA_RASTER_DIR, True, landcover_path,
                poll_dep_yield_nut_10km_path, poll_dep_yield_nut_10s_path,
                poll_dep_prod_nut_10s_path),
            target_path_list=[
                poll_dep_yield_nut_10km_path, poll_dep_yield_nut_10s_path,
                poll_dep_prod_nut_10s_path],
            task_name=f"""create poll dep production raster {
                os.path.basename(poll_dep_prod_nut_10s_path)}""")
        poll_dep_prod_nut_10s_task_path_map[nut_id] = (
            pol_dep_prod_task, poll_dep_prod_nut_10s_path)

    # The proportional area of natural within 2 km was calculated for every
    #  pixel of agricultural land (GLOBIO land-cover classes 2, 230, 231, and
    #  232) at 10 arc seconds (~300 m) resolution. This 2 km scale represents
    #  the distance most commonly found to be predictive of pollination
    #  services (Kennedy et al. 2013).
    kernel_raster_path = os.path.join(CHURN_DIR, 'radial_kernel.tif')
    kernel_task = task_graph.add_task(
        func=create_radial_convolution_mask,
        args=(0.00277778, 2000., kernel_raster_path),
        target_path_list=[kernel_raster_path],
        task_name='make convolution kernel')

    # This loop is so we don't duplicate code for each mask type with the
    # only difference being the lulc codes and prefix
    mask_task_path_map = {}
    for mask_prefix, globio_codes in [
            ('ag', GLOBIO_AG_CODES), ('hab', GLOBIO_NATURAL_CODES),
            ('bmp', BMP_LULC_CODES)]:
        mask_key = f'{landcover_key}_{mask_prefix}_mask'
        mask_target_path = os.path.join(
            CHURN_DIR, f'{mask_prefix}_mask',
            f'{mask_key}.tif')
        mask_task = task_graph.add_task(
            func=mask_raster,
            args=(landcover_path, globio_codes, mask_target_path),
            target_path_list=[mask_target_path],
            task_name=f'mask {mask_key}',)

        mask_task_path_map[mask_prefix] = (mask_task, mask_target_path)

    # blend bmp into ag
    for mask_prefix, bmp_frac in [('hab', 0.1), ('ag', 0.9)]:
        mask_key = f'{landcover_key}_{mask_prefix}_bmp_mask'
        combined_mask_bmp_path = os.path.join(
            CHURN_DIR, f'{mask_prefix}_mask',
            f'{mask_key}.tif')
        mask_task = task_graph.add_task(
            func=pygeoprocessing.raster_calculator,
            args=(
                [(mask_task_path_map[mask_prefix][1], 1),
                 (mask_task_path_map['bmp'][1], 1),
                 (bmp_frac, 'raw'),
                 (_MASK_NODATA, 'raw')],
                fractional_add_op,
                combined_mask_bmp_path,
                gdal.GDT_Float32, _MASK_NODATA),
            target_path_list=[combined_mask_bmp_path],
            dependent_task_list=[
                mask_task_path_map[mask_prefix][0],
                mask_task_path_map['bmp'][0]],
            task_name='combine bmp mask %s' % mask_key)
        mask_task_path_map[mask_prefix] = (mask_task, combined_mask_bmp_path)

    pollhab_2km_prop_path = os.path.join(
        CHURN_DIR, 'pollhab_2km_prop',
        f'pollhab_2km_prop_{landcover_key}.tif')
    pollhab_2km_prop_task = task_graph.add_task(
        func=pygeoprocessing.convolve_2d,
        args=[
            (mask_task_path_map['hab'][1], 1), (kernel_raster_path, 1),
            pollhab_2km_prop_path],
        kwargs={
            'working_dir': CHURN_DIR,
            'ignore_nodata': True,
            'n_threads': 4},
        dependent_task_list=[mask_task_path_map['hab'][0], kernel_task],
        target_path_list=[pollhab_2km_prop_path],
        task_name=(
            'calculate proportional'
            f' {os.path.basename(pollhab_2km_prop_path)}'))

    # calculate pollhab_2km_prop_on_ag_10s by multiplying pollhab_2km_prop
    # by the ag mask
    pollhab_2km_prop_on_ag_path = os.path.join(
        output_dir, f'''pollhab_2km_prop_on_ag_10s_{
            landcover_key}.tif''')
    pollhab_2km_prop_on_ag_task = task_graph.add_task(
        func=mult_rasters,
        args=(
            mask_task_path_map['ag'][1], pollhab_2km_prop_path,
            pollhab_2km_prop_on_ag_path),
        target_path_list=[pollhab_2km_prop_on_ag_path],
        dependent_task_list=[
            pollhab_2km_prop_task, mask_task_path_map['ag'][0]],
        task_name=(
            f'''pollhab 2km prop on ag {
                os.path.basename(pollhab_2km_prop_on_ag_path)}'''))

    #  1.1.4.  Sufficiency threshold A threshold of 0.3 was set to
    #  evaluate whether there was sufficient pollinator habitat in the 2
    #  km around farmland to provide pollination services, based on
    #  Kremen et al.'s (2005)  estimate of the area requirements for
    #  achieving full pollination. This produced a map of wild
    #  pollination sufficiency where every agricultural pixel was
    #  designated in a binary fashion: 0 if proportional area of habitat
    #  was less than 0.3; 1 if greater than 0.3. Maps of pollination
    #  sufficiency can be found at (permanent link to output), outputs
    #  "poll_suff_..." below.

    threshold_val = 0.3
    pollinator_suff_hab_path = os.path.join(
        CHURN_DIR, 'poll_suff_hab_ag_coverage_rasters',
        f'poll_suff_ag_coverage_prop_10s_{landcover_key}.tif')
    poll_suff_task = task_graph.add_task(
        func=threshold_select_raster,
        args=(
            pollhab_2km_prop_path,
            mask_task_path_map['ag'][1], threshold_val,
            pollinator_suff_hab_path),
        target_path_list=[pollinator_suff_hab_path],
        dependent_task_list=[
            pollhab_2km_prop_task, mask_task_path_map['ag'][0]],
        task_name=f"""poll_suff_ag_coverage_prop {
            os.path.basename(pollinator_suff_hab_path)}""")

    # tot_prod_en|va|fo_10s|1d_cur|ssp1|ssp3|ssp5
    # total annual production of energy (KJ/yr), vitamin A (IU/yr),
    # and folate (mg/yr)
    for nut_id in ('en', 'va', 'fo'):
        tot_prod_task, tot_prod_path = (
            prod_total_nut_10s_task_path_map[nut_id])

        prod_total_potential_path = os.path.join(
            output_dir, f'''prod_total_potential_{
                nut_id}_10s_{landcover_key}.tif''')
        prod_total_potential_task = task_graph.add_task(
            func=mult_rasters,
            args=(
                mask_task_path_map['ag'][1], tot_prod_path,
                prod_total_potential_path),
            target_path_list=[prod_total_potential_path],
            dependent_task_list=[tot_prod_task, mask_task_path_map['ag'][0]],
            task_name=(
                f'tot_prod_{nut_id}_10s_{landcover_key}'))

        poll_dep_prod_task, poll_dep_prod_path = (
            poll_dep_prod_nut_10s_task_path_map[nut_id])

        prod_poll_dep_potential_nut_scenario_path = os.path.join(
            output_dir,
            f'prod_poll_dep_potential_{nut_id}_10s_'
            f'{landcover_key}.tif')
        prod_poll_dep_potential_nut_scenario_task = task_graph.add_task(
            func=mult_rasters,
            args=(
                mask_task_path_map['ag'][1], poll_dep_prod_path,
                prod_poll_dep_potential_nut_scenario_path),
            target_path_list=[prod_poll_dep_potential_nut_scenario_path],
            dependent_task_list=[
                poll_dep_prod_task, mask_task_path_map['ag'][0]],
            task_name=(
                f'poll_dep_prod_{nut_id}_'
                f'10s_{landcover_key}'))

        # pollination independent
        prod_poll_indep_nut_scenario_path = os.path.join(
            output_dir,
            f'prod_poll_indep_{nut_id}_10s_'
            f'{landcover_key}.tif')
        prod_poll_indep_nut_scenario_task = task_graph.add_task(
            func=subtract_2_rasters,
            args=(
                prod_total_potential_path,
                prod_poll_dep_potential_nut_scenario_path,
                prod_poll_indep_nut_scenario_path),
            target_path_list=[prod_poll_indep_nut_scenario_path],
            dependent_task_list=[
                prod_total_potential_task,
                prod_poll_dep_potential_nut_scenario_task],
            task_name=(
                f'prod_poll_indep_{nut_id}_'
                f'10s_{landcover_key}'))

        # prod_poll_dep_realized_en|va|fo_10s|1d_cur|ssp1|ssp3|ssp5:
        # pollination-dependent annual production of energy (KJ/yr),
        # vitamin A (IU/yr), and folate (mg/yr) that can be met by wild
        # pollinators due to the proximity of sufficient habitat.
        prod_poll_dep_realized_nut_scenario_path = os.path.join(
            output_dir,
            f'prod_poll_dep_realized_{nut_id}_10s_'
            f'{landcover_key}.tif')
        prod_poll_dep_realized_nut_scenario_task = task_graph.add_task(
            func=mult_rasters,
            args=(
                prod_poll_dep_potential_nut_scenario_path,
                pollinator_suff_hab_path,
                prod_poll_dep_realized_nut_scenario_path),
            target_path_list=[prod_poll_dep_realized_nut_scenario_path],
            dependent_task_list=[
                poll_suff_task, prod_poll_dep_potential_nut_scenario_task],
            task_name=(
                f'prod_poll_dep_realized_{nut_id}_'
                f'10s_{landcover_key}'))

        # calculate prod_poll_dep_unrealized X1 as
        # prod_total - prod_poll_dep_realized
        prod_poll_dep_unrealized_nut_scenario_path = os.path.join(
            output_dir,
            f'prod_poll_dep_unrealized_{nut_id}_10s_'
            f'{landcover_key}.tif')
        prod_poll_dep_unrealized_nut_scenario_task = task_graph.add_task(
            func=pygeoprocessing.raster_calculator,
            args=([
                (prod_poll_dep_potential_nut_scenario_path, 1),
                (prod_poll_dep_realized_nut_scenario_path, 1),
                (_MULT_NODATA, 'raw'),
                (_MULT_NODATA, 'raw'),
                (_MULT_NODATA, 'raw')], sub_two_op,
                prod_poll_dep_unrealized_nut_scenario_path,
                gdal.GDT_Float32, -1),
            target_path_list=[prod_poll_dep_unrealized_nut_scenario_path],
            dependent_task_list=[
                prod_poll_dep_realized_nut_scenario_task,
                prod_poll_dep_potential_nut_scenario_task],
            task_name=f'''prod poll dep unrealized: {
                os.path.basename(
                    prod_poll_dep_unrealized_nut_scenario_path)}''')

        # calculate prod_total_realized as
        #   prod_total_potential - prod_poll_dep_unrealized
        prod_total_realized_nut_scenario_path = os.path.join(
            output_dir,
            f'prod_total_realized_{nut_id}_10s_'
            f'{landcover_key}.tif')
        prod_total_realized_nut_scenario_task = task_graph.add_task(
            func=pygeoprocessing.raster_calculator,
            args=([
                (prod_total_potential_path, 1),
                (prod_poll_dep_unrealized_nut_scenario_path, 1),
                (_MULT_NODATA, 'raw'),
                (_MULT_NODATA, 'raw'),
                (_MULT_NODATA, 'raw')], sub_two_op,
                prod_total_realized_nut_scenario_path,
                gdal.GDT_Float32, _MULT_NODATA),
            target_path_list=[prod_total_realized_nut_scenario_path],
            dependent_task_list=[
                prod_poll_dep_unrealized_nut_scenario_task,
                prod_total_potential_task],
            task_name=f'''prod poll dep unrealized: {
                os.path.basename(
                    prod_total_realized_nut_scenario_path)}''')

    # make prod poll dep value now:
    # tot_prod_en|va|fo_10s|1d_cur|ssp1|ssp3|ssp5
    # total annual production of energy (KJ/yr), vitamin A (IU/yr),
    # and folate (mg/yr)
    value_total_potential_path = os.path.join(
        output_dir, f'''value_total_potential_10s_{landcover_key}.tif''')
    value_total_potential_task = task_graph.add_task(
        func=mult_rasters,
        args=(
            mask_task_path_map['ag'][1], target_10s_value_path,
            value_total_potential_path),
        target_path_list=[value_total_potential_path],
        dependent_task_list=[mask_task_path_map['ag'][0]],
        task_name=(
            f'tot_value_10s_{landcover_key}'))

    value_poll_dep_potential_scenario_path = os.path.join(
        output_dir,
        f'value_poll_dep_potential_10s_'
        f'{landcover_key}.tif')
    value_poll_dep_potential_scenario_task = task_graph.add_task(
        func=mult_rasters,
        args=(
            mask_task_path_map['ag'][1], target_10s_poll_dep_value_path,
            value_poll_dep_potential_scenario_path),
        target_path_list=[value_poll_dep_potential_scenario_path],
        dependent_task_list=[mask_task_path_map['ag'][0]],
        task_name=(
            f'value_dep_prod_10s_{landcover_key}'))

    # pollination independent
    value_poll_indep_scenario_path = os.path.join(
        output_dir,
        f'value_poll_indep_10s_'
        f'{landcover_key}.tif')
    _ = task_graph.add_task(
        func=subtract_2_rasters,
        args=(
            value_total_potential_path,
            value_poll_dep_potential_scenario_path,
            value_poll_indep_scenario_path),
        target_path_list=[value_poll_indep_scenario_path],
        dependent_task_list=[
            value_total_potential_task,
            value_poll_dep_potential_scenario_task],
        task_name=(
            f'value_poll_indep_'
            f'10s_{landcover_key}'))

    # prod_poll_dep_realized_en|va|fo_10s|1d_cur|ssp1|ssp3|ssp5:
    # pollination-dependent annual production of energy (KJ/yr),
    # vitamin A (IU/yr), and folate (mg/yr) that can be met by wild
    # pollinators due to the proximity of sufficient habitat.
    value_poll_dep_realized_scenario_path = os.path.join(
        output_dir,
        f'value_poll_dep_realized_10s_'
        f'{landcover_key}.tif')
    value_poll_dep_realized_scenario_task = task_graph.add_task(
        func=mult_rasters,
        args=(
            value_poll_dep_potential_scenario_path,
            pollinator_suff_hab_path,
            value_poll_dep_realized_scenario_path),
        target_path_list=[value_poll_dep_realized_scenario_path],
        dependent_task_list=[
            poll_suff_task, value_poll_dep_potential_scenario_task],
        task_name=(
            f'value_poll_dep_realized_'
            f'10s_{landcover_key}'))

    # calculate value_poll_dep_unrealized X1 as
    # value_total - value_poll_dep_realized
    value_poll_dep_unrealized_scenario_path = os.path.join(
        output_dir,
        f'value_poll_dep_unrealized_10s_'
        f'{landcover_key}.tif')
    value_poll_dep_unrealized_scenario_task = task_graph.add_task(
        func=pygeoprocessing.raster_calculator,
        args=([
            (value_poll_dep_potential_scenario_path, 1),
            (value_poll_dep_realized_scenario_path, 1),
            (_MULT_NODATA, 'raw'),
            (_MULT_NODATA, 'raw'),
            (_MULT_NODATA, 'raw')], sub_two_op,
            value_poll_dep_unrealized_scenario_path,
            gdal.GDT_Float32, -1),
        target_path_list=[value_poll_dep_unrealized_scenario_path],
        dependent_task_list=[
            value_poll_dep_realized_scenario_task,
            value_poll_dep_potential_scenario_task],
        task_name=f'''prod poll dep unrealized: {
            os.path.basename(
                value_poll_dep_unrealized_scenario_path)}''')

    # calculate value_total_realized as
    #   value_total_potential - value_poll_dep_unrealized
    value_total_realized_scenario_path = os.path.join(
        output_dir,
        f'value_total_realized_10s_'
        f'{landcover_key}.tif')
    value_total_realized_scenario_task = task_graph.add_task(
        func=pygeoprocessing.raster_calculator,
        args=([
            (value_total_potential_path, 1),
            (value_poll_dep_unrealized_scenario_path, 1),
            (_MULT_NODATA, 'raw'),
            (_MULT_NODATA, 'raw'),
            (_MULT_NODATA, 'raw')], sub_two_op,
            value_total_realized_scenario_path,
            gdal.GDT_Float32, _MULT_NODATA),
        target_path_list=[value_total_realized_scenario_path],
        dependent_task_list=[
            value_poll_dep_unrealized_scenario_task,
            value_total_potential_task],
        task_name=f'''prod poll dep unrealized: {
            os.path.basename(
                value_total_realized_scenario_path)}''')

    task_graph.close()
    task_graph.join()


def build_spatial_index(vector_path):
    """Build an rtree/geom list tuple from ``vector_path``."""
    vector = gdal.OpenEx(vector_path)
    layer = vector.GetLayer()
    geom_index = rtree.index.Index()
    geom_list = []
    for index in range(layer.GetFeatureCount()):
        feature = layer.GetFeature(index)
        geom = feature.GetGeometryRef()
        shapely_geom = shapely.wkb.loads(geom.ExportToWkb())
        shapely_prep_geom = shapely.prepared.prep(shapely_geom)
        geom_list.append(shapely_prep_geom)
        geom_index.insert(index, shapely_geom.bounds)

    return geom_index, geom_list


def calculate_total_requirements(
        pop_path_list, nut_need_list, target_path):
    """Calculate total nutrient requirements.
    Create a new raster by summing all rasters in `pop_path_list` multiplied
    by their corresponding scalar in `nut_need_list`.
    Parameters:
        pop_path_list (list of str): list of paths to population counts.
        nut_need_list (list): list of scalars that correspond in order to
            the per-count nutrient needs of `pop_path_list`.
        target_path (str): path to target file.
    Return:
        None.
    """
    nodata = -1
    pop_nodata = pygeoprocessing.get_raster_info(
        pop_path_list[0])['nodata'][0]

    def mult_and_sum(*arg_list):
        """Arg list is an (array0, scalar0, array1, scalar1,...) list.
        Returns:
            array0*scalar0 + array1*scalar1 + .... but ignore nodata.
        """
        result = numpy.empty(arg_list[0].shape, dtype=numpy.float32)
        result[:] = nodata
        array_stack = numpy.array(arg_list[0::2])
        scalar_list = numpy.array(arg_list[1::2])
        # make a valid mask as big as a single array
        valid_mask = numpy.logical_and.reduce(
            array_stack != pop_nodata, axis=0)

        # mask out all invalid elements but reshape so there's still the same
        # number of arrays
        valid_array_elements = (
            array_stack[numpy.broadcast_to(valid_mask, array_stack.shape)])
        array_stack = None

        # sometimes this array is empty, check first before reshaping
        if valid_array_elements.size != 0:
            valid_array_elements = valid_array_elements.reshape(
                -1, numpy.count_nonzero(valid_mask))
            # multiply each element of the scalar with each row of the valid
            # array stack, then sum along the 0 axis to get the result
            result[valid_mask] = numpy.sum(
                (valid_array_elements.T * scalar_list).T, axis=0)
        scalar_list = None
        valid_mask = None
        valid_array_elements = None
        return result

    pygeoprocessing.raster_calculator(list(itertools.chain(*[
        ((path, 1), (scalar, 'raw')) for path, scalar in zip(
            pop_path_list, nut_need_list)])), mult_and_sum, target_path,
        gdal.GDT_Float32, nodata)


def sub_two_op(a_array, b_array, a_nodata, b_nodata, target_nodata):
    """Subtract a from b and ignore nodata."""
    result = numpy.empty_like(a_array)
    result[:] = target_nodata
    valid_mask = (a_array != a_nodata) & (b_array != b_nodata)
    result[valid_mask] = a_array[valid_mask] - b_array[valid_mask]
    return result


def average_rasters(*raster_list, clamp=None):
    """Average rasters in raster list except write to the last one.
    Parameters:
        raster_list (list of string): list of rasters to average over.
        clamp (float): value to clamp the individual raster to before the
            average.
    Returns:
        None.
    """
    nodata_list = [
        pygeoprocessing.get_raster_info(path)['nodata'][0]
        for path in raster_list[:-1]]
    target_nodata = -1.

    def average_op(*array_list):
        result = numpy.empty_like(array_list[0])
        result[:] = target_nodata
        valid_mask = numpy.ones(result.shape, dtype=numpy.bool)
        clamped_list = []
        for array, nodata in zip(array_list, nodata_list):
            valid_mask &= array != nodata
            if clamp:
                clamped_list.append(
                    numpy.where(array > clamp, clamp, array))
            else:
                clamped_list.append(array)

        if valid_mask.any():
            array_stack = numpy.stack(clamped_list)
            result[valid_mask] = numpy.average(
                array_stack[numpy.broadcast_to(
                    valid_mask, array_stack.shape)].reshape(
                        len(array_list), -1), axis=0)
        return result

    pygeoprocessing.raster_calculator(
        [(path, 1) for path in raster_list[:-1]], average_op,
        raster_list[-1], gdal.GDT_Float32, target_nodata)


def subtract_2_rasters(
        raster_path_a, raster_path_b, target_path):
    """Calculate target = a-b and ignore nodata."""
    a_nodata = pygeoprocessing.get_raster_info(raster_path_a)['nodata'][0]
    b_nodata = pygeoprocessing.get_raster_info(raster_path_b)['nodata'][0]
    target_nodata = -9999

    def sub_op(a_array, b_array):
        """Sub a-b-c as arrays."""
        result = numpy.empty(a_array.shape, dtype=numpy.float32)
        result[:] = target_nodata
        valid_mask = (
            ~numpy.isclose(a_array, a_nodata) &
            ~numpy.isclose(b_array, b_nodata))
        result[valid_mask] = (
            a_array[valid_mask] - b_array[valid_mask])
        return result

    pygeoprocessing.raster_calculator(
        [(raster_path_a, 1), (raster_path_b, 1)],
        sub_op, target_path, gdal.GDT_Float32, target_nodata)


def subtract_3_rasters(
        raster_path_a, raster_path_b, raster_path_c, target_path):
    """Calculate target = a-b-c and ignore nodata."""
    a_nodata = pygeoprocessing.get_raster_info(raster_path_a)['nodata'][0]
    b_nodata = pygeoprocessing.get_raster_info(raster_path_b)['nodata'][0]
    c_nodata = pygeoprocessing.get_raster_info(raster_path_c)['nodata'][0]
    target_nodata = -9999

    def sub_op(a_array, b_array, c_array):
        """Sub a-b-c as arrays."""
        result = numpy.empty(a_array.shape, dtype=numpy.float32)
        result[:] = target_nodata
        valid_mask = (
            (a_array != a_nodata) &
            (b_array != b_nodata) &
            (c_array != c_nodata))
        result[valid_mask] = (
            a_array[valid_mask] - b_array[valid_mask] - c_array[valid_mask])
        return result

    pygeoprocessing.raster_calculator(
        [(raster_path_a, 1), (raster_path_b, 1), (raster_path_c, 1)],
        sub_op, target_path, gdal.GDT_Float32, target_nodata)


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
    kernel_band.SetNoDataValue(NODATA)
    kernel_band.WriteArray(normalized_kernel_array)


def threshold_select_raster(
        base_raster_path, select_raster_path, threshold_val, target_path):
    """Select `select` if `base` >= `threshold_val`.
    Parameters:
        base_raster_path (string): path to single band raster that will be
            used to determine the threshold mask to select from
            `select_raster_path`.
        select_raster_path (string): path to single band raster to pass
            through to target if aligned `base` pixel is >= `threshold_val`
            0 otherwise, or nodata if base == nodata. Must be the same
            shape as `base_raster_path`.
        threshold_val (numeric): value to use as threshold cutoff
        target_path (string): path to desired output raster, raster is a
            byte type with same dimensions and projection as
            `base_raster_path`. A pixel in this raster will be `select` if
            the corresponding pixel in `base_raster_path` is >=
            `threshold_val`, 0 otherwise or nodata if `base` == nodata.
    Returns:
        None.
    """
    base_nodata = pygeoprocessing.get_raster_info(
        base_raster_path)['nodata'][0]
    target_nodata = -9999.

    def threshold_select_op(
            base_array, select_array, threshold_val, base_nodata,
            target_nodata):
        result = numpy.empty(select_array.shape, dtype=numpy.float32)
        result[:] = target_nodata
        valid_mask = (base_array != base_nodata) & (
            select_array >= 0) & (select_array <= 1)
        result[valid_mask] = select_array[valid_mask] * numpy.interp(
            base_array[valid_mask], [0, threshold_val], [0.0, 1.0], 0, 1)
        return result

    pygeoprocessing.raster_calculator(
        [(base_raster_path, 1), (select_raster_path, 1),
         (threshold_val, 'raw'), (base_nodata, 'raw'),
         (target_nodata, 'raw')], threshold_select_op,
        target_path, gdal.GDT_Float32, target_nodata)


def mask_raster(base_path, codes, target_path):
    """Mask `base_path` to 1 where values are in codes. 0 otherwise.
    Parameters:
        base_path (string): path to single band integer raster.
        codes (list): list of integer or tuple integer pairs. Membership in
            `codes` or within the inclusive range of a tuple in `codes`
            is sufficient to mask the corresponding raster integer value
            in `base_path` to 1 for `target_path`.
        target_path (string): path to desired mask raster. Any corresponding
            pixels in `base_path` that also match a value or range in
            `codes` will be masked to 1 in `target_path`. All other values
            are 0.
    Returns:
        None.
    """
    code_list = numpy.array([
        item for sublist in [
            range(x[0], x[1]+1) if isinstance(x, tuple) else [x]
            for x in codes] for item in sublist])
    LOGGER.debug(f'expanded code array {code_list}')

    base_nodata = pygeoprocessing.get_raster_info(base_path)['nodata'][0]

    def mask_codes_op(base_array, codes_array):
        """Return a bool raster if value in base_array is in codes_array."""
        result = numpy.empty(base_array.shape, dtype=numpy.int8)
        result[:] = _MASK_NODATA
        valid_mask = base_array != base_nodata
        result[valid_mask] = numpy.isin(
            base_array[valid_mask], codes_array)
        return result

    pygeoprocessing.raster_calculator(
        [(base_path, 1), (code_list, 'raw')], mask_codes_op, target_path,
        gdal.GDT_Byte, 2)


def unzip_file(zipfile_path, target_dir, touchfile_path):
    """Unzip contents of `zipfile_path`.
    Parameters:
        zipfile_path (string): path to a zipped file.
        target_dir (string): path to extract zip file to.
        touchfile_path (string): path to a file to create if unzipping is
            successful.
    Returns:
        None.
    """
    with zipfile.ZipFile(zipfile_path, 'r') as zip_ref:
        zip_ref.extractall(target_dir)

    with open(touchfile_path, 'w') as touchfile:
        touchfile.write(f'unzipped {zipfile_path}')


def _make_logger_callback(message):
    """Build a timed logger callback that prints `message` replaced.
    Parameters:
        message (string): a string that expects a %f placement variable,
            for % complete.
    Returns:
        Function with signature:
            logger_callback(df_complete, psz_message, p_progress_arg)
    """
    def logger_callback(df_complete, psz_message, p_progress_arg):
        """Log updates using GDAL API for callbacks."""
        try:
            current_time = time.time()
            if ((current_time - logger_callback.last_time) > 5.0 or
                    (df_complete == 1.0 and
                     logger_callback.total_time >= 5.0)):
                LOGGER.info(message, df_complete * 100)
                logger_callback.last_time = current_time
                logger_callback.total_time += current_time
        except AttributeError:
            logger_callback.last_time = time.time()
            logger_callback.total_time = 0.0

    return logger_callback


def total_yield_op(
        yield_nodata, yield_factor_list,
        *crop_yield_harea_array_list):
    """Calculate total yield.
    Parameters:
        yield_nodata (numeric): nodata value for the arrays in
            ``crop_yield_array_list```.
        yield_factor_list (list of float): list of factors per crop that
            convert to total yield.
        crop_yield_harea_array_list (list of numpy.ndarray): list of length
            2*n of 2D arrays of n yield (tons/Ha) for crops that correlate in
            order with the ``yield_factor_list`` followed by
            n harea (proportional area) of those crops.
    Returns:
        sum(crop_yield (tons/Ha) * harvested_area(1/1) *
        yield_factor)
    """
    result = numpy.empty(
        crop_yield_harea_array_list[0].shape, dtype=numpy.float32)
    result[:] = 0.0
    all_valid = numpy.zeros(result.shape, dtype=numpy.bool)

    n_crops = len(crop_yield_harea_array_list) // 2

    for crop_index in range(n_crops):
        crop_array = crop_yield_harea_array_list[crop_index]
        harea_array = crop_yield_harea_array_list[crop_index + n_crops]
        valid_mask = crop_array != yield_nodata
        all_valid |= valid_mask
        result[valid_mask] += (
            crop_array[valid_mask] * harea_array[valid_mask] *
            yield_factor_list[crop_index])
    result[~all_valid] = yield_nodata
    return result


def total_price_yield_op(
        yield_nodata, pollination_yield_factor_list,
        *crop_yield_harea_price_array_list):
    """Calculate total yield.
    Parameters:
        yield_nodata (numeric): nodata value for the arrays in
            ``crop_yield_array_list```.
        pollination_yield_factor_list (list of float): list of non-refuse
            proportion of yield that is pollination dependent.
        crop_yield_harea_price_array_list (list of numpy.ndarray): list of
            length 3*n of 2D arrays where
            * the first n is yield (tons/Ha) and
              the order of those crops correlate in order with the
              ``pollination_yield_factor_list``
            * followed at index n by n harea arrays (harvested area) of those
              crops
            * and ending at index 2n for n arrays which are the $/ton for those
              crops.
    Returns:
        sum(yield(tons/ha)*harvested_area(1/1)*pol_factor*price($/ton))
        (return units are $/ha)
    """
    result = numpy.empty(
        crop_yield_harea_price_array_list[0].shape, dtype=numpy.float32)
    result[:] = 0.0
    all_valid = numpy.zeros(result.shape, dtype=numpy.bool)

    n_crops = len(crop_yield_harea_price_array_list) // 3

    for crop_index in range(n_crops):
        crop_array = crop_yield_harea_price_array_list[crop_index]
        harea_array = crop_yield_harea_price_array_list[crop_index + n_crops]
        price_array = crop_yield_harea_price_array_list[crop_index + 2*n_crops]
        valid_mask = (
            (crop_array != yield_nodata) & (price_array != _MULT_NODATA) &
            (harea_array != _MULT_NODATA))
        all_valid |= valid_mask
        result[valid_mask] += (
            crop_array[valid_mask] * harea_array[valid_mask] *
            pollination_yield_factor_list[crop_index] *
            price_array[valid_mask])
    result[~all_valid] = yield_nodata
    return result


def total_cost_yield_op(
        yield_nodata, pollination_yield_factor_list,
        *crop_yield_harea_cost_array_list):
    """Calculate total yield.
    Parameters:
        yield_nodata (numeric): nodata value for the arrays in
            ``crop_yield_array_list```.
        pollination_yield_factor_list (list of float): list of non-refuse
            proportion of yield that is pollination dependent.
        crop_yield_harea_cost_array_list (list of numpy.ndarray): list of
            length 2n of 2D arrays where
            * the first n is harvested area and
              the order of those crops correlate in order with the
              ``pollination_yield_factor_list``
            * followed at index n by n cost arrays ($/ha) of those
              crops
    Returns:
        sum(harvested_area(1/1)*pol_factor*cost($/ha))
        (return units are $/ha)
    """
    result = numpy.empty(
        crop_yield_harea_cost_array_list[0].shape, dtype=numpy.float32)
    result[:] = 0.0
    all_valid = numpy.zeros(result.shape, dtype=numpy.bool)

    n_crops = len(crop_yield_harea_cost_array_list) // 2

    for crop_index in range(n_crops):
        harea_array = crop_yield_harea_cost_array_list[crop_index]
        cost_array = crop_yield_harea_cost_array_list[crop_index + n_crops]
        valid_mask = (
            (cost_array != _MULT_NODATA) & (harea_array != _MULT_NODATA))
        all_valid |= valid_mask
        result[valid_mask] += (
            harea_array[valid_mask] *
            pollination_yield_factor_list[crop_index] *
            cost_array[valid_mask])
    result[~all_valid] = yield_nodata
    return result


def density_to_value_op(density_array, area_array, density_nodata):
    """Calculate production.
    Parameters:
        density_array (numpy.ndarray): array of densities / area in
            ``area_array``.
        area_array (numpy.ndarray): area of each cell that corresponds with
            ``density_array``.
        density_ndoata (numeric): nodata value of the ``density_array``.
    """
    result = numpy.empty(density_array.shape, dtype=numpy.float32)
    result[:] = density_nodata
    valid_mask = density_array != density_nodata
    result[valid_mask] = density_array[valid_mask] * area_array[valid_mask]
    return result


def create_value_rasters(
        task_graph,
        valid_crop_set,
        crop_pol_dep_refuse_df_path, yield_and_harea_raster_dir,
        price_raster_dir, cost_raster_dir, consider_pollination,
        sample_target_raster_path, target_10km_value_yield_path,
        target_10s_value_yield_path,
        target_10s_value_path,
        target_10km_cost_yield_path,
        target_10s_cost_yield_path,
        target_10s_cost_path,
        target_10km_profit_yield_path,
        target_10s_profit_yield_path,
        target_10s_profit_path,
        target_total_harea_path):
    """Create an dollar value yield and total value raster for all crops.
    Parameters:
        valid_crop_set (set): set of Monfreda style crop ids that are the only
            crops that should be processed in this function.
        crop_pol_dep_refuse_df_path (str): path to CSV with at least the
            columns `filenm`, `Pollination dependence`.
        yield_and_harea_raster_dir (str): path to a directory that has files
            of the format `[crop_name]_yield.tif` and
            `[crop_name]_harea.tif` where `crop_name` is a value
            in the `filenm` column of `crop_nutrient_df`.
        price_raster_dir (str): path to a directory containing files of the
            form '[crop name]_price.tif' with units in $/ton.
        cost_raster_dir (str): path to a directory containing files of the form
            '[crop_id]_total_cost.tif'. These are in $/ha.
        consider_pollination (bool): if True, multiply yields by pollinator
            dependence ratio.
        sample_target_raster_path (path): path to a file that has the raster
            pixel size and dimensions of the desired
            `target_10s_value_path`.
        target_10km_value_yield_path (str): path to target raster that will
            contain a price yield raster ($/ha) for all crops.
        target_10s_value_yield_path (str): path to a resampled
            `target_10km_value_yield_path` at 10s resolution.
        target_10s_value_path (str): path to target raster that will
            contain a dollar amount of the total dollar value of crops per
            pixel.
        target_10km_cost_yield_path (str): path to target raster that will
            contain a cost yield raster ($/ha) for all crops.
        target_10s_cost_yield_path (str): path to a resampled
            `target_10km_cost_yield_path` at 10s resolution.
        target_10s_cost_path (str): path to target raster that will
            contain a dollar amount of the total dollar cost of crops per
            pixel.
        target_10km_profit_yield_path (str): path to target raster that will
            contain a profit yield raster ($/ha) for all crops.
        target_10s_profit_yield_path (str): path to a resampled
            `target_10km_profit_yield_path` at 10s resolution.
        target_10s_profit_path (str): path to target raster that will
            contain a dollar amount of the total dollar profit of crops per
            pixel.
        target_total_harea_path (str): path to total harvested area proportion
            for `valid_crop_set`.
    Returns:
        None.
    """
    for path in [
            target_10km_value_yield_path, target_10s_value_yield_path,
            target_10s_value_path,
            target_10km_cost_yield_path,
            target_10s_cost_yield_path,
            target_10s_cost_path]:
        try:
            os.makedirs(os.path.dirname(path))
        except OSError:
            pass
    crop_pol_dep_refuse_df = pandas.read_csv(crop_pol_dep_refuse_df_path)
    yield_raster_path_list = []
    harea_raster_path_list = []  # proportion of harvested area in pixel 0..1
    price_raster_path_list = []
    cost_raster_path_list = []
    pollination_yield_factor_list = []
    for _, row in crop_pol_dep_refuse_df.iterrows():
        crop_id = row['filenm']
        if crop_id not in valid_crop_set:
            continue
        pol_dep_prop = float(row['Pollination dependence'])
        yield_raster_path = os.path.join(
            yield_and_harea_raster_dir, "%s_yield.tif" % crop_id)
        harea_raster_path = os.path.join(
            yield_and_harea_raster_dir, "%s_harea.tif" % crop_id)
        price_raster_path = os.path.join(
            price_raster_dir, '%s_price.tif' % crop_id)
        cost_raster_path = os.path.join(
            cost_raster_dir, '%s_total_cost.tif' % crop_id)
        if os.path.exists(yield_raster_path):
            yield_raster_path_list.append(yield_raster_path)
            harea_raster_path_list.append(harea_raster_path)
            price_raster_path_list.append(price_raster_path)
            cost_raster_path_list.append(cost_raster_path)
            if consider_pollination:
                pollination_yield_factor_list.append(pol_dep_prop)
            else:
                pollination_yield_factor_list.append(1.0)
        else:
            raise ValueError(f"not found {yield_raster_path}")

    sample_target_raster_info = pygeoprocessing.get_raster_info(
        sample_target_raster_path)

    yield_raster_info = pygeoprocessing.get_raster_info(
        yield_raster_path_list[0])
    yield_nodata = yield_raster_info['nodata'][0]

    value_yield_10km_task = task_graph.add_task(
        func=pygeoprocessing.raster_calculator,
        args=(
            [(yield_nodata, 'raw'), (pollination_yield_factor_list, 'raw')] +
            [(x, 1) for x in yield_raster_path_list + harea_raster_path_list +
             price_raster_path_list], total_price_yield_op,
            target_10km_value_yield_path, gdal.GDT_Float32, yield_nodata),
        target_path_list=[target_10km_value_yield_path],
        task_name='calculate %s' % target_10km_value_yield_path)

    cost_yield_10km_task = task_graph.add_task(
        func=pygeoprocessing.raster_calculator,
        args=(
            [(yield_nodata, 'raw'), (pollination_yield_factor_list, 'raw')] +
            [(x, 1) for x in harea_raster_path_list +
             cost_raster_path_list], total_cost_yield_op,
            target_10km_cost_yield_path, gdal.GDT_Float32, yield_nodata),
        target_path_list=[target_10km_cost_yield_path],
        task_name='calculate %s' % target_10km_cost_yield_path)
    LOGGER.debug(harea_raster_path_list)
    LOGGER.debug(cost_raster_path_list)
    LOGGER.debug(
        '%s %s', pollination_yield_factor_list, target_10km_value_yield_path)
    task_graph.join()

    profit_yield_10km_task = task_graph.add_task(
        func=subtract_2_rasters,
        args=(
            target_10km_value_yield_path, target_10km_cost_yield_path,
            target_10km_profit_yield_path),
        target_path_list=[target_10km_profit_yield_path],
        dependent_task_list=[cost_yield_10km_task, value_yield_10km_task],
        task_name='calculate %s' % target_10km_profit_yield_path)

    total_harea_task = task_graph.add_task(
        func=pygeoprocessing.raster_calculator,
        args=(
            [(x, 1) for x in harea_raster_path_list] +
            [(-9999, 'raw') for _ in harea_raster_path_list] +
            [(_MULT_NODATA, 'raw')],
            sum_rasters_op,
            target_total_harea_path, gdal.GDT_Float32, _MULT_NODATA),
        target_path_list=[target_total_harea_path])

    y_lat_array = numpy.linspace(
        sample_target_raster_info['geotransform'][3],
        sample_target_raster_info['geotransform'][3] +
        sample_target_raster_info['geotransform'][5] *
        sample_target_raster_info['raster_size'][1],
        sample_target_raster_info['raster_size'][1])

    y_ha_array = area_of_pixel(
        abs(sample_target_raster_info['geotransform'][1]),
        y_lat_array) / 10000.0
    y_ha_column = y_ha_array.reshape((y_ha_array.size, 1))

    value_10s_yield_task = task_graph.add_task(
        func=pygeoprocessing.warp_raster,
        args=(
            target_10km_value_yield_path,
            sample_target_raster_info['pixel_size'],
            target_10s_value_yield_path,
            'cubicspline'),
        kwargs={
            'target_sr_wkt': sample_target_raster_info['projection'],
            'target_bb': sample_target_raster_info['bounding_box'],
            'n_threads': 2,
            },
        target_path_list=[target_10s_value_yield_path])

    cost_10s_yield_task = task_graph.add_task(
        func=pygeoprocessing.warp_raster,
        args=(
            target_10km_cost_yield_path,
            sample_target_raster_info['pixel_size'],
            target_10s_cost_yield_path,
            'cubicspline'),
        kwargs={
            'target_sr_wkt': sample_target_raster_info['projection'],
            'target_bb': sample_target_raster_info['bounding_box'],
            'n_threads': 2,
            },
        dependent_task_list=[cost_yield_10km_task],
        target_path_list=[target_10s_cost_yield_path])

    profit_10s_yield_task = task_graph.add_task(
        func=pygeoprocessing.warp_raster,
        args=(
            target_10km_profit_yield_path,
            sample_target_raster_info['pixel_size'],
            target_10s_profit_yield_path,
            'cubicspline'),
        kwargs={
            'target_sr_wkt': sample_target_raster_info['projection'],
            'target_bb': sample_target_raster_info['bounding_box'],
            'n_threads': 2,
            },
        dependent_task_list=[profit_yield_10km_task],
        target_path_list=[target_10s_profit_yield_path],
        task_name='warp 10km profit yield path')

    # multiply by area of pixel to get total production
    target_sr = osr.SpatialReference(sample_target_raster_info['projection'])
    if target_sr.IsProjected():
        pixel_prod_factor = numpy.array([[abs(
            numpy.prod(sample_target_raster_info['pixel_size']) / 1e4)]])
    else:
        pixel_prod_factor = y_ha_column

    task_graph.add_task(
        func=pygeoprocessing.raster_calculator,
        args=(
            [(target_10s_value_yield_path, 1), pixel_prod_factor,
             (yield_nodata, 'raw')], density_to_value_op,
            target_10s_value_path, gdal.GDT_Float32, yield_nodata),
        dependent_task_list=[value_10s_yield_task],
        target_path_list=[target_10s_value_path])

    task_graph.add_task(
        func=pygeoprocessing.raster_calculator,
        args=(
            [(target_10s_cost_yield_path, 1), pixel_prod_factor,
             (yield_nodata, 'raw')], density_to_value_op,
            target_10s_cost_path, gdal.GDT_Float32, yield_nodata),
        dependent_task_list=[cost_10s_yield_task],
        target_path_list=[target_10s_cost_path])

    task_graph.add_task(
        func=pygeoprocessing.raster_calculator,
        args=(
            [(target_10s_profit_yield_path, 1), pixel_prod_factor,
             (yield_nodata, 'raw')], density_to_value_op,
            target_10s_profit_path, gdal.GDT_Float32, yield_nodata),
        dependent_task_list=[profit_10s_yield_task],
        target_path_list=[target_10s_profit_path])
    task_graph.join()


def create_prod_nutrient_raster(
        valid_crop_set, crop_nutrient_df_path, nutrient_name,
        yield_and_harea_raster_dir,
        consider_pollination, sample_target_raster_path,
        target_10km_yield_path, target_10s_yield_path,
        target_10s_production_path):
    """Create total production & yield for a nutrient for all crops.
    Parameters:
        valid_crop_set (set): set of Monfreda style crop ids that are the only
            crops to process in this function.
        crop_nutrient_df_path (str): path to CSV with at least the
            column `filenm`, `nutrient_name`, `Percent refuse crop`, and
            `Pollination dependence crop`.
        nutrient_name (str): nutrient name to use to index into the crop
            data frame.
        yield_and_harea_raster_dir (str): path to a directory that has files
            of the format `[crop_name]_yield.tif` and
            `[crop_name]_harea.tif` where `crop_name` is a value
            in the `filenm` column of `crop_nutrient_df`.
        consider_pollination (bool): if True, multiply yields by pollinator
            dependence ratio.
        sample_target_raster_path (path): path to a file that has the raster
            pixel size and dimensions of the desired
            `target_10s_production_path`.
        target_10km_yield_path (str): path to target raster that will
            contain total yield (tons/Ha)
        target_10s_yield_path (str): path to a resampled
            `target_10km_yield_path` at 10s resolution.
        target_10s_production_path (str): path to target raster that will
            contain a per-pixel amount of pollinator produced `nutrient_name`
            calculated as the sum(
                crop_yield_map * (100-Percent refuse crop) *
                (Pollination dependence crop) * nutrient) * (ha / pixel map))
    Returns:
        None.
    """
    for path in [
            target_10km_yield_path, target_10s_yield_path,
            target_10s_production_path]:
        try:
            os.makedirs(os.path.dirname(path))
        except OSError:
            pass
    crop_nutrient_df = pandas.read_csv(crop_nutrient_df_path)
    yield_raster_path_list = []
    harea_raster_path_list = []
    pollination_nutrient_yield_factor_list = []
    for _, row in crop_nutrient_df.iterrows():
        crop_id = row['filenm']
        if crop_id not in valid_crop_set:
            continue
        yield_raster_path = os.path.join(
            yield_and_harea_raster_dir, f"{crop_id}_yield.tif")
        harea_raster_path = os.path.join(
            yield_and_harea_raster_dir, f"{crop_id}_harea.tif")
        if os.path.exists(yield_raster_path):
            yield_raster_path_list.append(yield_raster_path)
            harea_raster_path_list.append(harea_raster_path)
            pollination_nutrient_yield_factor_list.append(
                (1. - row['Percent refuse'] / 100.) * row[nutrient_name])
            if consider_pollination:
                pollination_nutrient_yield_factor_list[-1] *= (
                    row['Pollination dependence'])
        else:
            raise ValueError(f"not found {yield_raster_path}")

    sample_target_raster_info = pygeoprocessing.get_raster_info(
        sample_target_raster_path)

    yield_raster_info = pygeoprocessing.get_raster_info(
        yield_raster_path_list[0])
    yield_nodata = yield_raster_info['nodata'][0]

    pygeoprocessing.raster_calculator(
        [(yield_nodata, 'raw'),
         (pollination_nutrient_yield_factor_list, 'raw')] +
        [(x, 1) for x in yield_raster_path_list + harea_raster_path_list],
        total_yield_op, target_10km_yield_path, gdal.GDT_Float32,
        yield_nodata)

    y_lat_array = numpy.linspace(
        sample_target_raster_info['geotransform'][3],
        sample_target_raster_info['geotransform'][3] +
        sample_target_raster_info['geotransform'][5] *
        sample_target_raster_info['raster_size'][1],
        sample_target_raster_info['raster_size'][1])

    y_ha_array = area_of_pixel(
        abs(sample_target_raster_info['geotransform'][1]),
        y_lat_array) / 10000.0
    y_ha_column = y_ha_array.reshape((y_ha_array.size, 1))

    pygeoprocessing.warp_raster(
        target_10km_yield_path,
        sample_target_raster_info['pixel_size'], target_10s_yield_path,
        'cubicspline', target_sr_wkt=sample_target_raster_info['projection'],
        target_bb=sample_target_raster_info['bounding_box'],
        n_threads=2)

    target_sr = osr.SpatialReference(sample_target_raster_info['projection'])
    if target_sr.IsProjected():
        pixel_prod_factor = numpy.array([[1e4 * abs(
            numpy.prod(sample_target_raster_info['pixel_size']) / 1e4)]])
    else:
        pixel_prod_factor = y_ha_column * 1e4

    # multiplying the ha_array by 1e4 because the of yield are in
    # nutrient / 100g and yield is in Mg / ha.
    pygeoprocessing.raster_calculator(
        [(target_10s_yield_path, 1), pixel_prod_factor,
         (yield_nodata, 'raw')], density_to_value_op,
        target_10s_production_path, gdal.GDT_Float32, yield_nodata)


def area_of_pixel(pixel_size, center_lat):
    """Calculate m^2 area of a wgs84 square pixel.
    Adapted from: https://gis.stackexchange.com/a/127327/2397
    Parameters:
        pixel_size (float): length of side of pixel in degrees.
        center_lat (float): latitude of the center of the pixel. Note this
            value +/- half the `pixel-size` must not exceed 90/-90 degrees
            latitude or an invalid area will be calculated.
    Returns:
        Area of square pixel of side length `pixel_size` centered at
        `center_lat` in m^2.
    """
    a = 6378137  # meters
    b = 6356752.3142  # meters
    e = numpy.sqrt(1-(b/a)**2)
    area_list = []
    for f in [center_lat+pixel_size/2, center_lat-pixel_size/2]:
        zm = 1 - e*numpy.sin(numpy.radians(f))
        zp = 1 + e*numpy.sin(numpy.radians(f))
        area_list.append(
            numpy.pi * b**2 * (
                numpy.log(zp/zm) / (2*e) +
                numpy.sin(numpy.radians(f)) / (zp*zm)))
    return pixel_size / 360. * (area_list[0]-area_list[1])


def _mult_raster_op(array_a, array_b, nodata_a, nodata_b, target_nodata):
    """Multiply a by b and skip nodata."""
    result = numpy.empty(array_a.shape, dtype=numpy.float32)
    result[:] = target_nodata
    valid_mask = (array_a != nodata_a) & (array_b != nodata_b)
    result[valid_mask] = array_a[valid_mask] * array_b[valid_mask]
    return result


def mult_rasters(raster_a_path, raster_b_path, target_path):
    """Multiply a by b and skip nodata."""
    raster_info_a = pygeoprocessing.get_raster_info(raster_a_path)
    raster_info_b = pygeoprocessing.get_raster_info(raster_b_path)

    nodata_a = raster_info_a['nodata'][0]
    nodata_b = raster_info_b['nodata'][0]

    if raster_info_a['raster_size'] != raster_info_b['raster_size']:
        aligned_raster_a_path = (
            target_path + os.path.basename(raster_a_path) + '_aligned.tif')
        aligned_raster_b_path = (
            target_path + os.path.basename(raster_b_path) + '_aligned.tif')
        pygeoprocessing.align_and_resize_raster_stack(
            [raster_a_path, raster_b_path],
            [aligned_raster_a_path, aligned_raster_b_path],
            ['near'] * 2, raster_info_a['pixel_size'], 'intersection')
        raster_a_path = aligned_raster_a_path
        raster_b_path = aligned_raster_b_path

    pygeoprocessing.raster_calculator(
        [(raster_a_path, 1), (raster_b_path, 1), (nodata_a, 'raw'),
         (nodata_b, 'raw'), (_MULT_NODATA, 'raw')], _mult_raster_op,
        target_path, gdal.GDT_Float32, _MULT_NODATA)


def add_op(target_nodata, *array_list):
    """Add & return arrays in ``array_list`` but ignore ``target_nodata``."""
    result = numpy.zeros(array_list[0].shape, dtype=numpy.float32)
    valid_mask = numpy.zeros(result.shape, dtype=numpy.bool)
    for array in array_list:
        # nodata values will be < 0
        local_valid_mask = array >= 0
        valid_mask |= local_valid_mask
        result[local_valid_mask] += array[local_valid_mask]
    result[~valid_mask] = target_nodata
    return result


def sum_num_sum_denom(
        num1_array, num2_array, denom1_array, denom2_array, nodata):
    """Calculate sum of num divided by sum of denom."""
    result = numpy.empty_like(num1_array)
    result[:] = nodata
    valid_mask = (
        ~numpy.isclose(num1_array, nodata) &
        ~numpy.isclose(num2_array, nodata) &
        ~numpy.isclose(denom1_array, nodata) &
        ~numpy.isclose(denom2_array, nodata))
    result[valid_mask] = (
        num1_array[valid_mask] + num2_array[valid_mask]) / (
        denom1_array[valid_mask] + denom2_array[valid_mask] + 1e-9)
    return result


def avg_3_op(array_1, array_2, array_3, nodata):
    """Average 3 arrays. Skip nodata."""
    result = numpy.empty_like(array_1)
    result[:] = nodata
    valid_mask = (
        ~numpy.isclose(array_1, nodata) &
        ~numpy.isclose(array_2, nodata) &
        ~numpy.isclose(array_3, nodata))
    result[valid_mask] = (
        array_1[valid_mask] +
        array_2[valid_mask] +
        array_3[valid_mask]) / 3.
    return result


def weighted_avg_3_op(
        array_1, array_2, array_3,
        scalar_1, scalar_2, scalar_3,
        nodata):
    """Weighted average 3 arrays. Skip nodata."""
    result = numpy.empty_like(array_1)
    result[:] = nodata
    valid_mask = (
        ~numpy.isclose(array_1, nodata) &
        ~numpy.isclose(array_2, nodata) &
        ~numpy.isclose(array_3, nodata))
    result[valid_mask] = (
        array_1[valid_mask]/scalar_1 +
        array_2[valid_mask]/scalar_2 +
        array_3[valid_mask]/scalar_3) / 3.
    return result


def count_ge_one(array):
    """Return count of elements >= 1."""
    return numpy.count_nonzero(array >= 1)


def prop_diff_op(array_a, array_b, nodata):
    """Calculate prop change from a to b."""
    result = numpy.empty_like(array_a)
    result[:] = nodata
    valid_mask = (
        ~numpy.isclose(array_a, nodata) &
        ~numpy.isclose(array_b, nodata))
    # the 1e-12 is to prevent a divide by 0 error
    result[valid_mask] = (
        array_b[valid_mask] - array_a[valid_mask]) / (
            array_a[valid_mask] + 1e-12)
    return result


def build_lookup_from_csv(
        table_path, key_field, to_lower=True, warn_if_missing=True):
    """Read a CSV table into a dictionary indexed by `key_field`.
    Creates a dictionary from a CSV whose keys are unique entries in the CSV
    table under the column named by `key_field` and values are dictionaries
    indexed by the other columns in `table_path` including `key_field` whose
    values are the values on that row of the CSV table.
    Parameters:
        table_path (string): path to a CSV file containing at
            least the header key_field
        key_field: (string): a column in the CSV file at `table_path` that
            can uniquely identify each row in the table.
        to_lower (bool): if True, converts all unicode in the CSV,
            including headers and values to lowercase, otherwise uses raw
            string values.
        warn_if_missing (bool): If True, warnings are logged if there are
            empty headers or value rows.
    Returns:
        lookup_dict (dict): a dictionary of the form {
                key_field_0: {csv_header_0: value0, csv_header_1: value1...},
                key_field_1: {csv_header_0: valuea, csv_header_1: valueb...}
            }
        if `to_lower` all strings including key_fields and values are
        converted to lowercase unicode.
    """
    # Check if the file encoding is UTF-8 BOM first, related to issue
    # https://bitbucket.org/natcap/invest/issues/3832/invest-table-parsing-does-not-support-utf
    encoding = None
    with open(table_path) as file_obj:
        first_line = file_obj.readline()
        if first_line.startswith('\xef\xbb\xbf'):
            encoding = 'utf-8-sig'
    table = pandas.read_csv(
        table_path, sep=None, engine='python', encoding=encoding)
    header_row = list(table)
    try:  # no unicode() in python 3
        key_field = unicode(key_field)
    except NameError:
        pass
    if to_lower:
        key_field = key_field.lower()
        header_row = [
            x if not isinstance(x, str) else x.lower()
            for x in header_row]

    if key_field not in header_row:
        raise ValueError(
            '%s expected in %s for the CSV file at %s' % (
                key_field, header_row, table_path))
    if warn_if_missing and '' in header_row:
        LOGGER.warn(
            "There are empty strings in the header row at %s", table_path)

    key_index = header_row.index(key_field)
    lookup_dict = {}
    for index, row in table.iterrows():
        if to_lower:
            row = pandas.Series([
                x if not isinstance(x, str) else x.lower()
                for x in row])
        # check if every single element in the row is null
        if row.isnull().values.all():
            LOGGER.warn(
                "Encountered an entirely blank row on line %d", index+2)
            continue
        if row.isnull().values.any():
            row = row.fillna('')
        lookup_dict[row[key_index]] = dict(zip(header_row, row))
    return lookup_dict


def download_and_unzip(url, target_dir, target_token_path):
    """Download `url` to `target_dir` and touch `target_token_path`."""
    zipfile_path = os.path.join(target_dir, os.path.basename(url))
    LOGGER.debug('url %s, zipfile_path: %s', url, zipfile_path)
    ecoshard.download_url(url, zipfile_path)

    with zipfile.ZipFile(zipfile_path, 'r') as zip_ref:
        zip_ref.extractall(target_dir)

    with open(target_token_path, 'w') as touchfile:
        touchfile.write(f'unzipped {zipfile_path}')


def cost_table_to_raster(
        base_raster_path, country_vector_path, cost_table_path,
        crop_name, target_cost_raster_path):
    """Rasterize countries as prices.
    Parameters:
        base_raster_path (str): path to a raster that will be the base shape
            for `target_crop_price_ratser_path`.
        country_vector_path (str): path to country shapefile that has a
            field called `ISO3` that corresponds to the first key in
            `price_map`.
        cost_table_path (str): path to a table that has columns 3 columns in
            order of 'iso_name', 'crop', and 'cost'.
        target_cost_raster_path (str): a raster with pixel values
            corresponding to the country in which the pixel resides and
            the price of that crop in the country.
    Returns:
        None.
    """
    LOGGER.debug(
        'starting rasterization of %s', target_cost_raster_path)
    country_cost_df = pandas.read_csv(cost_table_path)
    iso_crop_cost_map = {
        (x[1][0], x[1][1]): float(x[1][2])
        for x in country_cost_df.iterrows()
    }
    pygeoprocessing.new_raster_from_base(
        base_raster_path, target_cost_raster_path, gdal.GDT_Float32,
        [_MULT_NODATA], fill_value_list=[_MULT_NODATA])
    memory_driver = ogr.GetDriverByName('MEMORY')
    country_vector = gdal.OpenEx(country_vector_path, gdal.OF_VECTOR)
    country_layer = country_vector.GetLayer()

    price_vector = memory_driver.CreateDataSource('price_vector')
    spat_ref = country_layer.GetSpatialRef()

    price_layer = price_vector.CreateLayer(
        'price_layer', spat_ref, ogr.wkbPolygon)
    price_layer.CreateField(ogr.FieldDefn('price', ogr.OFTReal))
    price_layer_defn = price_layer.GetLayerDefn()
    # add polygons to subset_layer
    price_layer.StartTransaction()
    for country_feature in country_layer:
        iso_name = country_feature.GetField('ISO3')
        if (iso_name, crop_name) in iso_crop_cost_map:
            country_geom = country_feature.GetGeometryRef()
            new_feature = ogr.Feature(price_layer_defn)
            new_feature.SetGeometry(country_geom.Clone())
            new_feature.SetField(
                'price', iso_crop_cost_map[(iso_name, crop_name)])
            price_layer.CreateFeature(new_feature)
    price_layer.CommitTransaction()

    target_crop_raster = gdal.OpenEx(
        target_cost_raster_path, gdal.OF_RASTER | gdal.GA_Update)
    gdal.RasterizeLayer(
        target_crop_raster, [1], price_layer,
        options=['ATTRIBUTE=price'])
    LOGGER.debug('finished rasterizing %s' % target_cost_raster_path)


# blend bmp into hab
def fractional_add_op(base_array, new_array, frac_val, nodata):
    """base+new*frac"""
    valid_mask = (base_array != nodata) | (new_array != nodata)
    result = numpy.empty(base_array.shape)
    result[:] = nodata
    result[valid_mask] = numpy.where(
        base_array[valid_mask] != nodata,
        base_array[valid_mask], 0) + frac_val * numpy.where(
        new_array[valid_mask] != nodata,
        new_array[valid_mask], 0)
    return result


def calculate_global_costs(
        valid_crop_set,
        ag_costs_table_path,
        prices_by_crop_and_country_table_path,
        country_region_iso_table_path,
        avg_global_labor_cost_table_path,
        avg_global_mach_cost_table_path,
        avg_global_n_cost_table_path,
        avg_global_p_cost_table_path,
        avg_global_k_cost_table_path,
        country_crop_price_table_path):
    """Parse a global crop prices and ag cost table into per-country prices.
    Parameters:
        valid_crop_set (set): set of Monfreda style crop ids that are the only
            crops to process in this function.
        ag_costs_table_path (str): path to a CSV that has
            'group', 'group_name', 'item', 'avg_N', 'avg_P', 'avg_K',
            'laborcost', 'actual_mach',  'low_mach'.
            where 'group' and 'group_name' identify the geographic region,
            'item' is the crop name and other headers correspond to fertilizer,
            labor, and machine prices.
        prices_by_crop_and_country_table_path (str): path to a CSV table with
            headers: 'earthstat_filename_prefix', '2010' to '2014', 'aadm0_a3'.
            This table is used to calculate the per-country price if one exists
            between 2010 and 2014 (most recent used). Also used to calculate
            regional per-crop averages
        country_region_iso_table_path (str): path to table that maps
            'Group_name', 'Area_name', and 'ISO3' for each
            region/country/iso code.
        avg_global_labor_cost_table_path (str): create a 2D table whose rows
            (and first column) correspond to crop name and columns correspond
            to geographic regions while values correspond to labor cost per Ha
            for that crop.
        avg_global_mach_cost_table_path (str):  create a 2D table whose rows
            (and first column) correspond to crop name and columns correspond
            to geographic regions while values correspond to machinery cost per
            Ha for that crop.
        avg_global_n_cost_table_path (str): table with 'iso', 'crop', and
            'N_cost' fields.
        avg_global_p_cost_table_path (str): table with 'iso', 'crop', and
            'P_cost' fields.
        avg_global_k_cost_table_path (str): table with 'iso', 'crop', and
            'K_cost' fields.
        country_crop_price_table_path (str): create table with columns
            'country', 'iso_name', 'crop', 'price'. Not sure of the price
            units.
    Returns:
        None
    """
    ag_costs_df = pandas.read_csv(ag_costs_table_path, skiprows=[1])
    ag_costs_df['item'] = ag_costs_df['item'].str.lower()
    region_names = (
        ag_costs_df[['group', 'group_name']].drop_duplicates().dropna(
            how='any'))
    l_per_ha_cost = ag_costs_df[
        ['group', 'item', 'laborcost']].drop_duplicates().dropna(how='any')
    m_per_ha_cost = ag_costs_df[
        ['group', 'item', 'actual_mach']].drop_duplicates().dropna(how='any')
    n_per_ha_cost = ag_costs_df[
        ['group', 'item', 'avg_N']].drop_duplicates().dropna(
            how='any')
    p_per_ha_cost = ag_costs_df[
        ['group', 'item', 'avg_P']].drop_duplicates().dropna(
            how='any')
    k_per_ha_cost = ag_costs_df[
        ['group', 'item', 'avg_K']].drop_duplicates().dropna(
            how='any')

    crop_prices_by_country_df = pandas.read_csv(
        prices_by_crop_and_country_table_path)
    country_region_iso_df = pandas.read_csv(
        country_region_iso_table_path, encoding="ISO-8859-1")
    iso_to_region_map = {
        x[1]['ISO3']: x[1]['Group_name']
        for x in country_region_iso_df.iterrows()
    }
    # rever the iso to region map so we can make regions specific to countries
    region_to_iso_map = collections.defaultdict(list)
    for iso_code, region in iso_to_region_map.items():
        region_to_iso_map[region].append(iso_code)

    country_to_crop_price_map = collections.defaultdict(dict)
    avg_region_to_crop_price_map = collections.defaultdict(
        lambda: collections.defaultdict(list))
    global_crop_price_map = collections.defaultdict(list)
    iso_name_set = set()
    for _, y in crop_prices_by_country_df.iterrows():
        iso_name = str(y[2])
        if iso_name not in iso_to_region_map:
            # Becky says we need only consider the countries in our table
            # so if there's an extra one it's probably small and/or we don't
            # care about it.
            continue
        region = iso_to_region_map[iso_name]
        iso_name_set.add(iso_name)
        crop_name = str(y[5])
        # only process crops that are specified in input
        if crop_name not in valid_crop_set:
            continue
        prices = (y[27:31]).dropna()
        if prices.size > 0:
            price = float(prices.tail(1))
            country_to_crop_price_map[iso_name][crop_name] = price
            avg_region_to_crop_price_map[region][crop_name].append(price)
            global_crop_price_map[crop_name].append(price)
    for iso_name, crop_name in itertools.product(
            iso_to_region_map, global_crop_price_map):
        region = iso_to_region_map[iso_name]
        if crop_name not in country_to_crop_price_map[iso_name]:
            value = avg_region_to_crop_price_map[region][crop_name]
            # might be a list from previous calculation, convert to avg
            if isinstance(value, list):
                if value == []:
                    value = global_crop_price_map[crop_name]
                    if isinstance(value, list):
                        if value == []:
                            LOGGER.error('global %s has no price', crop_name)
                            sys.exit()
                        value = numpy.mean(value)
                        global_crop_price_map[crop_name] = value
                    avg_region_to_crop_price_map[region][crop_name] = value
                else:
                    value = numpy.mean(value)
                    avg_region_to_crop_price_map[region][crop_name] = value
            country_to_crop_price_map[iso_name][crop_name] = value

    with open(country_crop_price_table_path, 'w') as country_crop_price_table:
        country_crop_price_table.write('iso_name,crop,price\n')
        for iso_name in sorted(country_to_crop_price_map):
            price_map = country_to_crop_price_map[iso_name]
            for crop_name in sorted(price_map):
                price = price_map[crop_name]
                country_crop_price_table.write(
                    '%s,%s,%s\n' % (iso_name, crop_name, price))

    calculate_global_average(
        region_names, region_to_iso_map, global_crop_price_map.keys(),
        l_per_ha_cost, 'laborcost', avg_global_labor_cost_table_path)

    calculate_global_average(
        region_names, region_to_iso_map, global_crop_price_map.keys(),
        m_per_ha_cost, 'actual_mach', avg_global_mach_cost_table_path,
        (9999, 5302))

    calculate_global_average(
        region_names, region_to_iso_map, global_crop_price_map.keys(),
        n_per_ha_cost, 'avg_N', avg_global_n_cost_table_path,
        (9999, 5302))

    calculate_global_average(
        region_names, region_to_iso_map, global_crop_price_map.keys(),
        p_per_ha_cost, 'avg_P', avg_global_p_cost_table_path,
        (9999, 5302))

    calculate_global_average(
        region_names, region_to_iso_map, global_crop_price_map.keys(),
        k_per_ha_cost, 'avg_K', avg_global_k_cost_table_path,
        (9999, 5302))


def calculate_global_average(
        region_names, region_to_iso_map, crop_names, group_crop_cost_df,
        cost_id, target_table_path, remap_group_id_tuple=None):
    """Calculate `cost_id` average in `group_crop_cost_df` for all crops.
    Parameters:
        region_names (set): set of geographic region names.
        region_to_iso_map (dict): map the region in `region_names` to a list
            of country iso codes.
        crop_names (set): iterable of crop names to calculate on.
        group_crop_cost_df (pandas.DataFrame): contains columns 'group',
            'item', and the value of `cost_id`. Used to map the cost of a crop
            onto a region.
        cost_id (str): the cost column in `group_crop_cost_df`.
        target_table_path (str): path to table to create that will contain
            columns 'iso_name', 'crop', and the value of `cost_id`.
        remap_grop_id_tuple (tuple): if not None, remaps the value of 'group'
            in `group_crop_cost_df` in the first element to the second element.
            This was added to account for the "9999" group of China that we
            could remap to East Asia.
    Returns:
        None.
    """
    avg_global_price_map = {}
    for row in region_names.iterrows():
        group_name = row[1][1]
        group_id = row[1][0]
        if remap_group_id_tuple and group_id == remap_group_id_tuple[0]:
            group_id = remap_group_id_tuple[1]
        # get the subset of group/item/labor price for this group
        crop_group = group_crop_cost_df.loc[
            group_crop_cost_df['group'] == group_id]
        known_crops = crop_group['item'].str.lower()
        avg_cost = crop_group[cost_id].mean()
        avg_global_price_map[group_name] = {}
        for crop_name in sorted(crop_names):
            if (known_crops.isin([crop_name])).any():
                cost = float(
                    crop_group.loc[crop_group['item'] == crop_name][cost_id])
            else:
                cost = avg_cost
            avg_global_price_map[group_name][crop_name] = cost

    with open(target_table_path, 'w') as cost_table:
        cost_table.write('iso_name,crop,%s\n' % cost_id)
        for region in sorted(avg_global_price_map):
            for iso_code in sorted(region_to_iso_map[region]):
                for crop_name in sorted(crop_names):
                    if crop_name not in avg_global_price_map[region]:
                        avg_cost = numpy.mean(
                            [avg_global_price_map[x][crop_name]
                             for x in avg_global_price_map
                             if crop_name in avg_global_price_map[x]])
                    else:
                        avg_cost = avg_global_price_map[region][crop_name]
                    cost_table.write('%s,"%s",%f\n' % (
                        iso_code, crop_name, avg_cost))


def download_data(task_graph):
    """Download data required for analysis."""
    crop_nutrient_table_fetch_task = task_graph.add_task(
        func=ecoshard.download_url,
        args=(
            CROP_NUTRIENT_URL, CROP_NUTRIENT_TABLE_PATH),
        target_path_list=[CROP_NUTRIENT_TABLE_PATH],
        task_name=f'fetch {os.path.basename(CROP_NUTRIENT_TABLE_PATH)}')

    country_region_iso_table_fetch_task = task_graph.add_task(
        func=ecoshard.download_url,
        args=(
            COUNTRY_REGION_ISO_TABLE_URL, COUNTRY_REGION_ISO_TABLE_PATH),
        target_path_list=[COUNTRY_REGION_ISO_TABLE_PATH],
        task_name=f'fetch {os.path.basename(COUNTRY_REGION_ISO_TABLE_PATH)}')

    yield_and_harea_zip_touch_file_path = os.path.join(
        ECOSHARD_DIR, 'monfreda_2008_observed_yield_and_harea.COMPLETE')
    yield_and_harea_dl_zip_task = task_graph.add_task(
        func=download_and_unzip,
        args=(YIELD_AND_HAREA_ZIP_URL, ECOSHARD_DIR,
              yield_and_harea_zip_touch_file_path),
        target_path_list=[yield_and_harea_zip_touch_file_path],
        task_name='download and unzip yield and harea')

    fert_usage_touch_file_path = os.path.join(
        ECOSHARD_DIR, 'Fertilizer2000toMarijn_geotiff.COMPLETE')
    fert_usage_dl_zip_task = task_graph.add_task(
        func=download_and_unzip,
        args=(FERT_USAGE_RASTERS_URL, ECOSHARD_DIR,
              fert_usage_touch_file_path),
        target_path_list=[fert_usage_touch_file_path],
        task_name='download and unzip fertilizer')

    cbi_mod_yield_touch_file_path = os.path.join(
        ECOSHARD_DIR, 'cbi_mod_yield_tables.COMPLETE')
    cbi_mod_yield_dl_zip_task = task_graph.add_task(
        func=download_and_unzip,
        args=(CBI_MOD_YIELD_TABLES_URL, CBI_MOD_YIELD_TABLES_DIR,
              cbi_mod_yield_touch_file_path),
        target_path_list=[cbi_mod_yield_touch_file_path],
        task_name='download and unzip cbi mod yield')

    ag_cost_table_task = task_graph.add_task(
        func=ecoshard.download_url,
        args=(AG_COST_TABLE_URL, AG_COSTS_TABLE_PATH),
        target_path_list=[AG_COSTS_TABLE_PATH],
        task_name='download ag cost table')

    prices_by_crop_and_country_table_path = os.path.join(
        ECOSHARD_DIR, os.path.basename(PRICES_BY_CROP_AND_COUNTRY_TABLE_PATH))
    ag_cost_table_task = task_graph.add_task(
        func=ecoshard.download_url,
        args=(
            PRICES_BY_CROP_AND_COUNTRY_TABLE_URL,
            prices_by_crop_and_country_table_path),
        target_path_list=[prices_by_crop_and_country_table_path],
        task_name='country to region table')

    country_iso_gpkg_path = os.path.join(
        ECOSHARD_DIR, os.path.basename(COUNTRY_ISO_GPKG_URL))
    country_iso_gpkg_task = task_graph.add_task(
        func=ecoshard.download_url,
        args=(COUNTRY_ISO_GPKG_URL, country_iso_gpkg_path),
        target_path_list=[country_iso_gpkg_path],
        task_name='download country iso gpkg')
    task_graph.join()


def preprocess_data(task_graph, valid_crop_set):
    """Download all ecoshards and create base tables.
    Parameter:
        task_graph (taskgraph.TaskGraph): taskgraph object for scheduling,
            will use to block this function until complete.
        valid_crop_set (set): set of Monfreda style crop ids. These are the
            only crops that will be processed in this function.
    Returns:
        None.
    """
    calc_global_costs_task = task_graph.add_task(
        func=calculate_global_costs,
        args=(
            valid_crop_set,
            AG_COSTS_TABLE_PATH,
            PRICES_BY_CROP_AND_COUNTRY_TABLE_PATH,
            COUNTRY_REGION_ISO_TABLE_PATH,
            AVG_GLOBAL_LABOR_COST_TABLE_PATH,
            AVG_GLOBAL_MACH_COST_TABLE_PATH,
            AVG_GLOBAL_N_COST_TABLE_PATH,
            AVG_GLOBAL_P_COST_TABLE_PATH,
            AVG_GLOBAL_K_COST_TABLE_PATH,
            COUNTRY_CROP_PRICE_TABLE_PATH),
        target_path_list=[
            AVG_GLOBAL_LABOR_COST_TABLE_PATH,
            AVG_GLOBAL_MACH_COST_TABLE_PATH,
            AVG_GLOBAL_N_COST_TABLE_PATH,
            AVG_GLOBAL_P_COST_TABLE_PATH,
            AVG_GLOBAL_K_COST_TABLE_PATH,
            COUNTRY_CROP_PRICE_TABLE_PATH],
        hash_algorithm='md5',
        task_name='calc global costs')
    calc_global_costs_task.join()

    # fert cost is irrespective of crop despite our table showing it's per crop
    # it was just easier to make that table then we could rasterize a single
    # country as the global
    price_raster_task_list = []
    base_abaca_raster_path = os.path.join(
            YIELD_AND_HAREA_RASTER_DIR, 'abaca_yield.tif')
    fert_cost_raster_path_map = {}
    try:
        # this makes the crop cost AND working directories
        os.makedirs(CROP_COSTS_WORKING_DIR)
    except OSError:
        pass

    for fert_type, fert_table_path in [
            ('avg_N', AVG_GLOBAL_N_COST_TABLE_PATH),
            ('avg_P', AVG_GLOBAL_P_COST_TABLE_PATH),
            ('avg_K', AVG_GLOBAL_K_COST_TABLE_PATH)]:
        fert_cost_raster_path = os.path.join(
            CROP_COSTS_WORKING_DIR, 'global_%s.tif' % (fert_type,))
        fert_cost_raster_path_map[fert_type] = fert_cost_raster_path
        # pick any crop that's valid because fert costs are all the same per
        # crop
        valid_crop_id = next(iter(valid_crop_set))
        price_raster_task = task_graph.add_task(
            func=cost_table_to_raster,
            args=(
                # yield_raster_path is only used as a base raster for getting
                # the shape & size consisten
                base_abaca_raster_path, COUNTRY_ISO_GPKG_PATH,
                fert_table_path, valid_crop_id,
                fert_cost_raster_path),
            ignore_path_list=[COUNTRY_ISO_GPKG_PATH],
            hash_algorithm='md5',
            target_path_list=[fert_cost_raster_path],
            task_name='%s %s raster' % ('global', fert_type))
        price_raster_task_list.append(price_raster_task)

    # Create global price rasters
    for k_app_rate_path in glob.glob(os.path.join(
            FERT_USAGE_DIR, '*K2Oapprate.tif')):
        crop_name = re.match(
            '([^_]+)K2Oapprate\.tif', os.path.basename(k_app_rate_path))[1]
        # only process crops specified in the input
        if crop_name not in valid_crop_set:
            continue
        yield_raster_path = os.path.join(
            YIELD_AND_HAREA_RASTER_DIR, '%s_yield.tif' % crop_name)
        p_app_rate_path = os.path.join(
            FERT_USAGE_DIR, '%sP2O5apprate.tif' % crop_name)
        n_app_rate_path = os.path.join(
            FERT_USAGE_DIR, '%sNapprate.tif' % crop_name)
        if not os.path.exists(yield_raster_path):
            LOGGER.debug('skipping fert raster called %s', crop_name)
            continue

        total_fert_cost_raster_path = os.path.join(
            CROP_COSTS_WORKING_DIR, '%s_fert_cost_rate.tif' % crop_name)

        raster_path_band_list = [
            (k_app_rate_path, 1),
            (p_app_rate_path, 1),
            (n_app_rate_path, 1),
            (fert_cost_raster_path_map['avg_K'], 1),
            (fert_cost_raster_path_map['avg_P'], 1),
            (fert_cost_raster_path_map['avg_N'], 1)]

        nodata_list = [
            (_MULT_NODATA, 'raw') for path_band in raster_path_band_list]

        # the 1e-3 comes from the fact that app rates are in kg/ha but cost
        # is $/Mg
        fert_cost_task = task_graph.add_task(
            func=pygeoprocessing.raster_calculator,
            args=(
                [(1e-3, 'raw')] + raster_path_band_list + nodata_list +
                [(_MULT_NODATA, 'raw')],
                dot_prod_op, total_fert_cost_raster_path, gdal.GDT_Float32,
                _MULT_NODATA),
            dependent_task_list=price_raster_task_list,
            target_path_list=[total_fert_cost_raster_path],
            task_name='fert cost for %s' % crop_name)

        cost_raster_path_map = {}
        for cost_id, working_dir, cost_table_path in [
                ('laborcost', CROP_COSTS_WORKING_DIR,
                 AVG_GLOBAL_LABOR_COST_TABLE_PATH),
                ('actual_mach', CROP_COSTS_WORKING_DIR,
                 AVG_GLOBAL_MACH_COST_TABLE_PATH),
                ('price', CROP_PRICE_DIR,
                 COUNTRY_CROP_PRICE_TABLE_PATH)]:
            cost_raster_path = os.path.join(
                working_dir, '%s_%s.tif' % (crop_name, cost_id))
            cost_raster_path_map[cost_id] = cost_raster_path
            price_raster_task = task_graph.add_task(
                func=cost_table_to_raster,
                args=(
                    # base_abaca_raster_path is only used as a base raster for
                    # getting the shape & size consisten
                    base_abaca_raster_path, COUNTRY_ISO_GPKG_PATH,
                    cost_table_path, crop_name,
                    cost_raster_path),
                ignore_path_list=[COUNTRY_ISO_GPKG_PATH],
                hash_algorithm='md5',
                target_path_list=[cost_raster_path],
                task_name='%s %s raster' % (crop_name, cost_id))
            price_raster_task_list.append(price_raster_task)
        task_graph.join()

        total_cost_path = os.path.join(
            CROP_COSTS_DIR, '%s_total_cost.tif' % crop_name)
        cost_raster_path_band_list = [
            (total_fert_cost_raster_path, 1),
            (cost_raster_path_map['laborcost'], 1),
            (cost_raster_path_map['actual_mach'], 1)]
        nodata_list = [
            (_MULT_NODATA, 'raw') for _ in cost_raster_path_band_list]
        total_cost_raster_task = task_graph.add_task(
            func=pygeoprocessing.raster_calculator,
            args=(
                cost_raster_path_band_list + nodata_list +
                [(_MULT_NODATA, 'raw')],
                sum_rasters_op,
                total_cost_path,
                gdal.GDT_Float32,
                _MULT_NODATA),
            dependent_task_list=price_raster_task_list + [fert_cost_task],
            target_path_list=[total_cost_path],
            task_name='total cost for %s' % crop_name)
    # need to download everything before we can iterate through it
    task_graph.join()


def sum_rasters_op(*raster_nodata_list):
    """Sum all non-nodata values.
    Parameters:
        raster_nodata_list (list): list of 2n+1 length where the first n
            elements are raster array values and the second n elements are the
            nodata values for that array. The last element is the target
            nodata.
    Returns:
        sum(raster_nodata_list[0:n]) -- while accounting for nodata.
    """
    result = numpy.zeros(raster_nodata_list[0].shape, dtype=numpy.float32)
    nodata_mask = numpy.zeros(raster_nodata_list[0].shape, dtype=numpy.bool)
    n = len(raster_nodata_list) // 2
    for index in range(n):
        valid_mask = ~numpy.isclose(
            raster_nodata_list[index], raster_nodata_list[index+n])
        nodata_mask |= ~valid_mask
        result[valid_mask] += raster_nodata_list[index][valid_mask]
    result[nodata_mask] = raster_nodata_list[-1]
    return result


def dot_prod_op(scalar, *raster_nodata_list):
    """Do a dot product of vectors A*B.
    Parameters:
        scalar (float): value to multiply each pair by.
        raster_nodata_list (list): list of 4*n+1 length where the first n
            elements are from vector A, the next n are B, and last 2n are
            nodata values for those elements in order.
    Returns:
        A*B and nodata where it overlaps.
    """
    n_elements = (len(raster_nodata_list)-1) // 4
    result = numpy.zeros(raster_nodata_list[0].shape, dtype=numpy.float32)
    nodata_mask = numpy.zeros(result.shape, dtype=numpy.bool)
    for index in range(n_elements*2):
        nodata_mask |= numpy.isclose(
            raster_nodata_list[index],
            raster_nodata_list[index+n_elements*2]) | numpy.isnan(
            raster_nodata_list[index])
    for index in range(n_elements):
        result[~nodata_mask] += (
            scalar * raster_nodata_list[index][~nodata_mask] *
            raster_nodata_list[index+n_elements][~nodata_mask])
    result[nodata_mask] = raster_nodata_list[-1]
    return result


def calculate_valid_crop_set():
    """Return a set of monfreda style cropname that are valid."""
    # use this to get a set of known crop ids
    crop_nutrient_df = pandas.read_csv(CROP_NUTRIENT_TABLE_PATH)
    crop_nutrient_id_set = set()
    for _, row in crop_nutrient_df.iterrows():
        crop_nutrient_id_set.add(row['filenm'])

    # use this to determine which fert app rates are known
    fert_rate_crop_id_set = set()
    for k_app_rate_path in glob.glob(os.path.join(
            FERT_USAGE_DIR, '*K2Oapprate.tif')):
        crop_name = re.match(
            '([^_]+)K2Oapprate\.tif', os.path.basename(k_app_rate_path))[1]
        fert_rate_crop_id_set.add(crop_name)

    monfreda_crop_id_set = set()
    for yield_path in glob.glob(os.path.join(
            YIELD_AND_HAREA_RASTER_DIR, '*_yield.tif')):
        crop_name = re.match(
            '([^_]+)_yield\.tif', os.path.basename(yield_path))[1]
        monfreda_crop_id_set.add(crop_name)

    crop_prices_by_country_df = pandas.read_csv(
        PRICES_BY_CROP_AND_COUNTRY_TABLE_PATH)
    price_crop_id_set = set(crop_prices_by_country_df[
        'earthstat_filename_prefix'].unique())
    return (
        crop_nutrient_id_set & fert_rate_crop_id_set & monfreda_crop_id_set &
        price_crop_id_set)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NCI Pollination Analysis')
    parser.add_argument(
        'landcover rasters', type=str, nargs='+',
        help=(
            'Paths, patterns, or ecoshards to landcover rasters that use ESA '
            'ESA encoding.'))
    args = parser.parse_args()
    landcover_raster_list = []

    task_graph = taskgraph.TaskGraph(
        WORKING_DIR, N_WORKERS, reporting_interval=5.0)
    LOGGER.info("download data and preprocess")
    for dir_path in [
            ECOSHARD_DIR, CHURN_DIR, CBI_MOD_YIELD_TABLES_DIR,
            CBI_MOD_YIELD_RASTERS_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    for file_pattern in vars(args)['landcover rasters']:
        # might be a url
        if file_pattern.startswith('http'):
            target_path = os.path.join(
                ECOSHARD_DIR, os.path.basename(file_pattern))
            fetch_task = task_graph.add_task(
                func=ecoshard.download_url,
                args=(
                    file_pattern, target_path),
                target_path_list=[target_path],
                task_name=f'fetch {os.path.basename(target_path)}')
            fetch_task.join()
            glob_pattern = target_path
        else:
            glob_pattern = file_pattern

        # default is glob:
        for raster_path in glob.glob(glob_pattern):
            # just try to open it in case it's not a raster, we won't see
            # anything otherwise
            r = gdal.OpenEx(raster_path, gdal.OF_RASTER)
            if r is None:
                continue
            r = None
            landcover_raster_list.append(raster_path)

    if not landcover_raster_list:
        raise ValueError('no valid files were found')

    download_data(task_graph)
    valid_crop_set = calculate_valid_crop_set()
    # valid_crop_set_iter = iter(sorted(valid_crop_set))
    # valid_crop_set = set([
    #     next(valid_crop_set_iter) for _ in range(len(valid_crop_set)//20)])
    LOGGER.debug('running %d crops', len(valid_crop_set))
    time.sleep(1.0)
    preprocess_data(task_graph, valid_crop_set)
    for landcover_path in landcover_raster_list:
        LOGGER.info("process landcover map: %s", landcover_path)
        calculate_for_landcover(task_graph, landcover_path, valid_crop_set)

    task_graph.join()
    task_graph.close()