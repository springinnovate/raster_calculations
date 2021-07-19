#docker run -it --rm -v "%CD%":/usr/local/workspace -v "C:\Users\Becky\Documents\cnc_project\supporting_layers":/data therealspring/inspring:latest extract_band.py

from osgeo import gdal
import pygeoprocessing

def _passthrough_op(array):
    return array

raster_path = "/data/GDP_PPP.tif"
base_name = os.path.basename(os.path.splitext(raster_path)[0])
raster_info = pygeoprocessing.get_raster_info(raster_path)

for band_id in range(3, 4): #always go up one from the band # you want
    pygeoprocessing.raster_calculator(
        [(raster_path, band_id)],
        _passthrough_op, f'{base_name}{band_id}.tif', raster_info["datatype"], raster_info["nodata"][band_id-1])
