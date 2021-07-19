zonal_stats_by_raster.py [path to zonal raster] [path to value raster to summarize]

Will output a table in the same directory that summarize the mean, max, min of value raster within each code of the zonal raster (e.g., land cover or other zones).

Full example:
zonal_stats_by_raster.py "C:\Users\Becky\Documents\unilever\carbon_edge_model\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2014-v2.0.7_smooth_compressed.tif" "C:\Users\Becky\Documents\unilever\scenarios\September\regression\optimal_mask_350000000.0.tif"
