import gdal

path = "path_to_a_raster"
raster = gdal.OpenEx(path, gdal.OF_RASTER)
gtiff_driver = gdal.GetDriverByName("GTiff")
target_path = 'path_to_target_geotiff.tif'
gtiff_driver.CreateCopy(target_path, raster)
