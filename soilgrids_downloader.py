import gdal
import pygeoprocessing


if __name__ == '__main__':
	print('starting')
	url = '/vsicurl?max_retry=3&retry_delay=1&list_dir=no&url=https://files.isric.org/soilgrids/latest/data/bdod/bdod_0-5cm_mean.vrt'
	r = gdal.OpenEx(url)
	print(r)
	b = r.GetRasterBand(1)
	print(b.ReadAsArray(1000, 2000, 10, 10))
	