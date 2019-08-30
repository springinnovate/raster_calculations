"""Count non-zero non-nodata pixels in raster, get percentile values, and sum above each percentile to build the CDF."""
import sys
import os
import shutil
import logging
import multiprocessing

import pygeoprocessing
import numpy
import raster_calculations_core
from osgeo import gdal
import taskgraph

gdal.SetCacheMax(2**30)

WORKSPACE_DIR = 'CNC_workspace'
NCPUS = -1
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
    # [0.0, 0.0, 2.6564152339677546e-05, 0.00449669105901578, 0.026592994668002544, 0.08908325455615322, 0.21252896986988581, 0.4257240946680402, 0.8519801985470177, 1.1987215681382737, 1.54221074228756]
    path = r"C:\Users\Becky\Documents\raster_calculations\aggregate_realized_ES_score_nspntg_renorm_md5_f788b5b627aa06c4028a2277da9d8dc0.tif"
    nodata_value = pygeoprocessing.get_raster_info(path)['nodata'][0]
    top2_sum = 0.0
    top5_sum = 0.0
    top10_sum = 0.0
    top20_sum = 0.0
    top30_sum = 0.0
    top40_sum = 0.0
    top50_sum = 0.0
    top60_sum = 0.0
    top70_sum = 0.0
    top80_sum = 0.0
    top90_sum = 0.0
    full_sum = 0.0

    for _, block_data in pygeoprocessing.iterblocks((path, 1)):
        nodata_mask = numpy.isclose(block_data, nodata_value)
        top2_mask = block_data > 1.54221074228756
        top2_sum += numpy.sum(block_data[top2_mask & ~nodata_mask])
        top5_mask = block_data > 1.1987215681382737
        top5_sum += numpy.sum(block_data[top5_mask & ~nodata_mask])
        top10_mask = block_data > 0.8519801985470177
        top10_sum += numpy.sum(block_data[top10_mask & ~nodata_mask])
        top20_mask = block_data > 0.4257240946680402
        top20_sum += numpy.sum(block_data[top20_mask & ~nodata_mask])
        top30_mask = block_data > 0.21252896986988581
        top30_sum += numpy.sum(block_data[top30_mask & ~nodata_mask])
        top40_mask = block_data > 0.08908325455615322
        top40_sum += numpy.sum(block_data[top40_mask & ~nodata_mask])
        top50_mask = block_data > 0.026592994668002544
        top50_sum += numpy.sum(block_data[top50_mask & ~nodata_mask])
        top60_mask = block_data > 0.00449669105901578
        top60_sum += numpy.sum(block_data[top60_mask & ~nodata_mask])
        top70_mask = block_data > 2.6564152339677546e-05
        top70_sum += numpy.sum(block_data[top70_mask & ~nodata_mask])
        nonzero_mask = block_data != 0
        full_sum += numpy.sum(block_data[nonzero_mask & ~nodata_mask])

    print(
        'Pixel count stats from %s\n'
        '2.5 pct sum:                    %14.2f\n'
        '5 pct sum:                      %14.2f\n'
        '10 pct sum:                     %14.2f\n'
        '20 pct sum:                     %14.2f\n'
        '30 pct sum:                     %14.2f\n'
        '40 pct sum:                     %14.2f\n'
        '50 pct sum:                     %14.2f\n'
        '60 pct sum:                     %14.2f\n'
        '70 pct sum:                     %14.2f\n'
        '100 pct sum:                    %14.2f\n' % (
            path, top2_sum, top5_sum, top10_sum, top20_sum, top30_sum, top40_sum, top50_sum, top60_sum, top70_sum, full_sum))

#2.5 pct sum:                       77750003.43
#5 pct sum:                        130085623.90
#10 pct sum:                       209758688.42
#20 pct sum:                       304675563.91
#30 pct sum:                       352506707.61
#40 pct sum:                       375005156.25
#50 pct sum:                       383134918.72
#60 pct sum:                       385359011.24
#70 pct sum:                       385546722.25
#100 pct sum:                      385546979.30
    
    return  # terminates at this point


    #path = r"C:\Users\Becky\Documents\raster_calculations\aggregate_realized_ES_score_nspntg_renorm_md5_f788b5b627aa06c4028a2277da9d8dc0.tif"
    path = r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\masked_nathab_esa_md5_40577bae3ef60519b1043bb8582a07af.tif"
    # gets the nodata value from the first band ([0]) of `path`
    nodata_value = pygeoprocessing.get_raster_info(path)['nodata'][0]
    # loop over all memory blocks of the first band of path (indicated by
    # the (path, 1) tuple, and ignore the second argument from iterblocks that
    # shows what block it is (that's the `_`)
    nonzero_count = 0
    total_pixels = 0
    nodata_count = 0
    running_sum = 0.0
    for _, block_data in pygeoprocessing.iterblocks((path, 1)):
        # we'll use this nodata mask to mask only valid nonzero counts and
        # also to count the number of nodata in the raster
        nodata_mask = numpy.isclose(block_data, nodata_value)
        # make a mask where the raster block is != 0 AND is not equal to a
        # nodata value
        nonzero_mask = block_data != 0
        nonzero_count += numpy.count_nonzero(nonzero_mask & ~nodata_mask)

        # only get the valid numbers for the sum
        running_sum += numpy.sum(block_data[nonzero_mask & ~nodata_mask])

        # count # of nodata pixels
        nodata_count += numpy.count_nonzero(nodata_mask)

        # and count for the total size of the block
        total_pixels += block_data.size

    # this is fine:
    print(
        'Pixel count stats from %s\n'
        'total pixels:                   %11d\n'
        'nonzero non-nodata pixel count: %11d\n'
        'nodata count:                   %11d\n'
        'sum:                            %14.2f\n' % (
            path, total_pixels, nonzero_count, nodata_count, running_sum))
    
    return

    #print(
    #    'Pixel count stats from %s\n'
    #    'total pixels:                   %11d\n'
    #    'nonzero non-nodata pixel count: %11d\n' % (
    #        path, total_pixels, nonzero_count))

    ## for aggregate_realized_ES_score_nspntg_renorm_md5_f788b5b627aa06c4028a2277da9d8dc0
    #total pixels:                    6531840000
    #nonzero non-nodata pixel count:  1133004447
    #nodata count:                    5118894498

    ## for masked_nathab_esa_md5_40577bae3ef60519b1043bb8582a07af.tif
    #total pixels:                    8398080000
    #nonzero non-nodata pixel count:  1257421938
    #nodata count:                             0
    #sum:                                      0.00

    #So 1/10 of 1257421938 is 125742194 <-- the number of pixels at this resolution making up 10% of the remaining natural habitat land area
    # For aggregate ES, that corresponds to 125742194/1133004447 is 0.1109812007648722. So if we want the top 11th percentile we need to take the 0.89

    nathab_path = r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\masked_nathab_esa_md5_40577bae3ef60519b1043bb8582a07af.tif"
    nathab_nodata_value = pygeoprocessing.get_raster_info(nathab_path)['nodata'][0]
    nathab_nonzero_count = 0
    for _, nathab_block_data in pygeoprocessing.iterblocks((path, 1)):
        nathab_nodata_mask = numpy.isclose(nathab_block_data, nathab_nodata_value)
        nathab_nonzero_mask = nathab_block_data != 0
        nathab_nonzero_count += numpy.count_nonzero(nathab_nonzero_mask & ~nathab_nodata_mask)


    pct_path = r"C:\Users\Becky\Documents\raster_calculations\aggregate_realized_ES_score_nspntg_renorm_md5_f788b5b627aa06c4028a2277da9d8dc0.tif"
    percentile_working_dir = r"C:\Users\Becky\Documents\raster_calculations\percentile_working_dir"
    try:
        os.makedirs(percentile_working_dir)
    except OSError:
        pass
    percentile_values_list = pygeoprocessing.raster_band_percentile((pct_path, 1), percentile_working_dir, [1, 12, 23, 34, 45, 56, 67, 78, 89, 94.5, 97.25])
    shutil.rmtree(percentile_working_dir)
    print(percentile_values_list)

    # [0.0, 0.0, 2.6564152339677546e-05, 0.00449669105901578, 0.026592994668002544, 0.08908325455615322, 0.21252896986988581, 0.4257240946680402, 0.8519801985470177, 1.1987215681382737, 1.54221074228756]
    
    

if __name__ == '__main__':
    main()