from osgeo import gdal
import pygeoprocessing

def _passthrough_op(array):
    return array

for band_id in range(1, 17):
    pygeoprocessing.raster_calculator(
        [("baccini_carbon_data_2003_2014_compressed_md5_11d1455ee8f091bf4be12c4f7ff9451b.tif", band_id)],
        _passthrough_op, f'baccini_band{band_id}.tif', gdal.GDT_Int16, 65535)
