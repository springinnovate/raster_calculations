from osgeo import gdal
import pygeoprocessing

pygeoprocessing.raster_calculator(
    [("baccini_carbon_data_2003_2014_compressed_md5_11d1455ee8f091bf4be12c4f7ff9451b.tif", 12)],
    lambda x: x, 'baccini_band12.tif', gdal.GDT_Int16, 65535)
