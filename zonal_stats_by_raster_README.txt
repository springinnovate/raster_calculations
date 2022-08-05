zonal_stats_by_raster.py [path to zonal raster] [path to value raster to summarize]

Will output a table in the same directory that summarize the mean, max, min of value raster within each code of the zonal raster (e.g., land cover or other zones).

Full example:
python zonal_stats_by_raster.py "C:\Users\Becky\Documents\unilever\carbon_edge_model\ESACCI-LC-L4-LCCS-Map-300m-P1Y-2014-v2.0.7_smooth_compressed.tif" "C:\Users\Becky\Documents\unilever\scenarios\September\regression\optimal_mask_350000000.0.tif"

python zonal_stats_by_raster.py "D:\ecoshard\Ecoregions2017_ESA2020modVCFv2_zones_compressed_md5_ab2aa0.tif" "D:\ecoshard\Manageable_Carbon_2018\Vulnerable_C_Total_2018_WARPED_near_md5_9ab63337d8b4a6c6fd4f7f597a66ffed.tif" --basename VC --do_not_align