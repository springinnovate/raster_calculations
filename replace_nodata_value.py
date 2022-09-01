"""Replace the nodata value in a raster."""
import argparse
import logging
import sys

from osgeo import gdal

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Replace nodata value in a raster.')
    parser.add_argument('raster_path', help='Path to any raster.')
    parser.add_argument('new_nodata', type=float, help='Nodata value to replace.')
    parser.add_argument('--band_index', type=int, default=1, help='Band to replace, defaults to 1.')
    args = parser.parse_args()

    raster = gdal.OpenEx(args.raster_path, gdal.OF_RASTER | gdal.GA_Update)
    band = raster.GetRasterBand(args.band_index)
    LOGGER.info(f'raster {args.raster_path} at band index {args.band_index} current nodata is {band.GetNoDataValue()}')
    band.SetNoDataValue(args.new_nodata)
    LOGGER.info(f'raster {args.raster_path} at band index {args.band_index} NEW nodata is {band.GetNoDataValue()}')
    band = None
    raster = None
