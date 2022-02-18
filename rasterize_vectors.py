from ecoshard import geoprocessing
import os
import gdal

if __name__ == '__main__':
    vector_path_list = (
        r"C:\Users\Becky\Documents\countries_iso3_md5_6fb2431e911401992e6e56ddf0a9bcda.gpkg",
        )

    #To make a mask
    #for vector_path in vector_path_list:
    #   print(vector_path)
    #   raster_path = "%s.tif" % os.path.splitext(vector_path)[0]
    #   geoprocessing.create_raster_from_vector_extents(
    #       vector_path, raster_path, (0.008333333333, -0.008333333333), gdal.GDT_Byte, 2, fill_value=2
    #       # (cell size, -cell size), gdal.GDT_Int32 OR _Byte OR ..., nodata value, filling up the raster with nodata values to start
    #   geoprocessing.rasterize(
    #       vector_path, raster_path, burn_values=[1])
    #       # adds the polygons to a preexisting raster; burnvalue is the value of the pixels that have a polygon on them


    # To make a raster of the shapefile's field
    for vector_path in vector_path_list:
        print(vector_path)
        raster_path = "%s.tif" % os.path.splitext(vector_path)[0]
        geoprocessing.create_raster_from_vector_extents(
            #vector_path, raster_path, (0.002777777777778, -0.002777777777778), gdal.GDT_Int32, -1, fill_value=-1)
            vector_path, raster_path, (0.002777777777778, -0.002777777777778), gdal.GDT_Int32, -1, fill_value=-1)

        # (cell size, -cell size), gdal.GDT_Int32 OR _Byte OR ..., nodata value, filling up the raster with nodata values to start
        geoprocessing.rasterize(
            vector_path, raster_path, option_list=["ATTRIBUTE=id"] # put the column that has the values that you want in the raster as the ATTRIBUTE
            #if there's polygon that lay on top of each other, the default the one on top wins; can use option_list=["ATTRIBUTE=aol, "MERGE_ALG=ADD"]
            #to keep both, or...,"ALL_TOUCHED=TRUE"] to include pixels that fall outside the center of the polygon
        )
            # adds the polygons to a preexisting raster; burnvalue is the value of the pixels that have a polygon on them


    #0.008333333333 #is 1 km
    #0.002 # is 300 m
