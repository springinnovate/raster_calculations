"""Check differences in raster overlaps."""
from ecoshard import geoprocessing
from osgeo import gdal
import numpy


base_raster = r"C:\Users\richp\Documents\code_repos\raster_calculations\zonal_stats_workspace\zonal_stats\tmpj8i1__ju\agg_fid_0.tif"

other_rasters = [
    r"C:\Users\richp\Documents\code_repos\raster_calculations\zonal_stats_workspace\zonal_stats\tmpzraeuuo5\agg_fid_1.tif",
    r"C:\Users\richp\Documents\code_repos\raster_calculations\zonal_stats_workspace\zonal_stats\tmpzraeuuo5\agg_fid_2.tif",
    r"C:\Users\richp\Documents\code_repos\raster_calculations\zonal_stats_workspace\zonal_stats\tmpzraeuuo5\agg_fid_3.tif",
    r"C:\Users\richp\Documents\code_repos\raster_calculations\zonal_stats_workspace\zonal_stats\tmpzraeuuo5\agg_fid_4.tif",
    r"C:\Users\richp\Documents\code_repos\raster_calculations\zonal_stats_workspace\zonal_stats\tmpzraeuuo5\agg_fid_5.tif",
    r"C:\Users\richp\Documents\code_repos\raster_calculations\zonal_stats_workspace\zonal_stats\tmpzraeuuo5\agg_fid_6.tif",
    r"C:\Users\richp\Documents\code_repos\raster_calculations\zonal_stats_workspace\zonal_stats\tmpzraeuuo5\agg_fid_0.tif",
]

NODATA = geoprocessing.get_raster_info(base_raster)['nodata'][0]


def mask_diff(*array_list):
    nodata_array_list = [array != NODATA for array in array_list[1:]]
    joined_array = numpy.any(nodata_array_list, axis=(0,))
    print(joined_array)
    print(array_list[0] != NODATA)
    base_nodata_array = array_list[0] != NODATA
    return base_nodata_array != joined_array

if __name__ == '__main__':

    geoprocessing.raster_calculator(
        [(base_raster, 1)] + [(path, 1) for path in other_rasters], mask_diff,
        'diff.tif', gdal.GDT_Byte, -1,)
    sdflkj

    raster_list = [
        gdal.OpenEx(path, gdal.OF_RASTER) for path in other_rasters]
    band_list = [r.GetRasterBand(1) for r in raster_list]
    nodata_list = [band.GetNoDataValue() for band in band_list]
    base_nodata = geoprocessing.get_raster_info(base_raster)['nodata'][0]
    for offset_dict, base_array in geoprocessing.iterblocks((base_raster, 1)):
        print(offset_dict)
        array_list = [
            band.ReadAsArray(**offset_dict) != nodata
            for band, nodata in zip(band_list, nodata_list)]
        joined_array = numpy.any(array_list, axis=(0,))
        print(joined_array)
        print(base_array != base_nodata)
