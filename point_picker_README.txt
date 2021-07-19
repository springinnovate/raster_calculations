python point_picker.py [path to rasters *.tif] [path to shapefile]

Will make a new shapefile in the same directory as `point_picker.py` named the
same as the directory that contains the path to rasters. The contents of that
shapefile will be any points that lie within any of the raster bounds and the
values of the points will contain fields named after the basenames of the
rasters. If any points lie over any rasters the value of the fields will be
the pixel value of the raster where the point intersects it.

Full example:

python point_picker.py "C:\Users\richp\Documents\code_repos\one_off_scripts\point_picker\data\align_inputs\*.tif" "C:\Users\richp\Documents\code_repos\one_off_scripts\point_picker\data\merged_forest_plot_data.shp"

python point_picker.py "C:\Users\Becky\Documents\handy_scripts_for_becky_from_rich\gobi\TNC_ecosystem_types.tif" "C:\Users\Becky\Documents\handy_scripts_for_becky_from_rich\gobi\sites_2016.gpkg"

python point_picker.py "C:\Users\Becky\Documents\unilever\files_to_summarize\*.tif" "C:\Users\Becky\Documents\unilever\merged_forest_plot_data.gpkg"


python point_picker.py "C:\Users\Becky\Documents\unilever\scenarios\September\biomass_per_ha_stocks_landcover_type_mask_ESACCI-LC-L4-LCCS-Map-300m-P1Y-2014-v2.0.7_smooth_compressed.tif" "C:\Users\Becky\Dropbox\NatCap\projects\unilever\carbon_UMN\Validation\merged_forest_plot_data\merged_forest_plot_data.gpkg"

python point_picker.py "C:\Users\Becky\Documents\unilever\scenarios\baccini_10s_2014_md5_5956a9d06d4dffc89517cefb0f6bb008_ovr.tif" "C:\Users\Becky\Documents\raster_calculations\handy_scripts_for_becky_from_rich\September.gpkg"

python point_picker.py "C:\Users\Becky\Documents\unilever\scenarios\ipcc\ipcc_carbon_esa2014_ovr.tif" "C:\Users\Becky\Documents\raster_calculations\handy_scripts_for_becky_from_rich\scenarios.gpkg"

python point_picker.py "C:\Users\Becky\Documents\unilever\carbon_edge_model\ESA2014_mask.tif" "C:\Users\Becky\Documents\raster_calculations\handy_scripts_for_becky_from_rich\ipcc.gpkg"

python point_picker.py "C:\Users\Becky\Documents\raster_calculations\handy_scripts_for_becky_from_rich\carbon_validation\ecozone_country_intersect.tif" "C:\Users\Becky\Documents\raster_calculations\handy_scripts_for_becky_from_rich\carbon_edge_model.gpkg"

python point_picker.py "C:\Users\Becky\Documents\eo-pollination\eo_pollination_data\eo_workspace\*.tif" "C:\Users\Becky\Dropbox\NatCap\projects\NASA GEOBON\data\Pollination\validation_data\turrialba.gpkg"

python point_picker.py "C:\Users\Becky\Documents\eo-pollination\eo_pollination_data\eo_workspace\total*t2.tif" "C:\Users\Becky\Dropbox\NatCap\projects\NASA GEOBON\data\Pollination\validation_data\PZ-sampling_sites.gpkg"