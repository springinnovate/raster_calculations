"""Count non-zero pixels in raster."""
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
    """Entry point."""
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
        running_sum = numpy.sum(block_data[nonzero_mask & ~nodata_mask])

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

    nathab_path = r"C:\Users\Becky\Documents\raster_calculations\CNC_workspace\masked_nathab_esa_md5_40577bae3ef60519b1043bb8582a07af.tif"
    nathab_nodata_value = pygeoprocessing.get_raster_info(nathab_path)['nodata'][0]
    nathab_nonzero_count = 0
    nathab_total_pixels = 0
    nathab_nodata_count = 0
    nathab_running_sum = 0.0
    for _, nathab_block_data in pygeoprocessing.iterblocks((path, 1)):
        nathab_nodata_mask = numpy.isclose(nathab_block_data, nathab_nodata_value)
        nathab_nonzero_mask = nathab_block_data != 0
        nathab_nonzero_count += numpy.count_nonzero(nathab_nonzero_mask & ~nathab_nodata_mask)


    path = r"C:\Users\Becky\Documents\raster_calculations\aggregate_realized_ES_score_nspntg_renorm_md5_f788b5b627aa06c4028a2277da9d8dc0.tif"
    percentile_working_dir = r"C:\Users\Becky\Documents\raster_calculations\percentile_working_dir"
    #makes a temporary directory because there's a shitton of rasters to find out the percentiles
    try:
        os.makedirs(percentile_working_dir)
    except OSError:
        pass
        #checks to see if the directory already exists, if it doesn't it makes it, if it does it doesn't do anything
    percentile_values_list = pygeoprocessing.raster_band_percentile(
        (path, 1), percentile_working_dir, [0, 1, 25, 50, 75, 95, 99, 100])
    # (path,1) is indicating the first band in that "path" raster; the 2nd argument is the working dir; the third is the list of percentiles we want
    shutil.rmtree(percentile_working_dir)
    #this gets rid of that termporary directory
    print(percentile_values_list)
    return  # terminates at this point

if __name__ == '__main__':
    main()