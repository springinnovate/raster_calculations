"""Calculate real area of raster mask under polygon."""
import argparse
import logging
import sys

from osgeo import gdal

gdal.SetCacheMax(2**28)

logging.basicConfig(
    level=logging.INFO,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)

LOGGER = logging.getLogger(__name__)


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(
        description='Calculate real area of raster mask under polygon.')
    parser.add_argument('raster_path', help='Raster path')
    parser.add_argument('vector_path', help='Vector path')
    args = parser.parse_args()


if __name__ == '__main__':
    main()
