import pygeoprocessing

raster_path = r"./workspace/ecoshards/ESACCI-LC-L4-LCCS-Map-300m-P1Y-2014-v2.0.7_smooth_compressed.tif"
raster_path = r"./workspace/ecoshards/baccini_carbon_data_2003_2014_compressed_md5_11d1455ee8f091bf4be12c4f7ff9451b"
raster_info = pygeoprocessing.get_raster_info(raster_path)
pygeoprocessing.warp_raster(
    raster_path, raster_info['pixel_size'], 'input_lulc.tif', 'near',
    target_bb=[-64, -4, -55, 3])
