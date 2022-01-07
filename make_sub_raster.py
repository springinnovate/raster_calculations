"""Clip out a small section of a raster to use for debugging."""
import argparse
import glob
import logging
import os
import random

from ecoshard import geoprocessing
from osgeo import gdal
import numpy

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(__name__)


WORLD_ECKERT_IV_WKT = """PROJCRS["unknown",
    BASEGEOGCRS["GCS_unknown",
        DATUM["World Geodetic System 1984",
            ELLIPSOID["WGS 84",6378137,298.257223563,
                LENGTHUNIT["metre",1]],
            ID["EPSG",6326]],
        PRIMEM["Greenwich",0,
            ANGLEUNIT["Degree",0.0174532925199433]]],
    CONVERSION["unnamed",
        METHOD["Eckert IV"],
        PARAMETER["Longitude of natural origin",0,
            ANGLEUNIT["Degree",0.0174532925199433],
            ID["EPSG",8802]],
        PARAMETER["False easting",0,
            LENGTHUNIT["metre",1],
            ID["EPSG",8806]],
        PARAMETER["False northing",0,
            ID["EPSG",8807]]],
    CS[Cartesian,2],
        AXIS["(E)",east,
            ORDER[1],
            LENGTHUNIT["metre",1,
                ID["EPSG",9001]]],
        AXIS["(N)",north,
            ORDER[2],
            LENGTHUNIT["metre",1,
                ID["EPSG",9001]]]]"""


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(description='Clip out a sample raster.')
    parser.add_argument(
        'raster_path_pattern', nargs='+', help='path or pattern to rasters '
        'to clip')
    parser.add_argument(
        '--target_dir', required=True, help='path to target directory')
    parser.add_argument(
        '--target_dims', default=(256, 256), nargs=2,
        help='target dimensions, defaults to 256x256')
    args = parser.parse_args()

    os.makedirs(args.target_dir, exist_ok=True)

    for path_pattern in args.raster_path_pattern:
        for raster_path in glob.glob(path_pattern):
            basename = os.path.basename(raster_path)
            LOGGER.info(f'processing {raster_path}')
            raster_info = geoprocessing.get_raster_info(raster_path)
            n_cols, n_rows = raster_info['raster_size']
            raster = gdal.OpenEx(raster_path, gdal.OF_RASTER)
            band = raster.GetRasterBand(1)
            nodata = band.GetNoDataValue()
            while True:
                offset_dict = {
                    'xoff': random.randrange(0, n_cols-args.target_dims[0]),
                    'yoff': random.randrange(0, n_rows-args.target_dims[1]),
                    'win_xsize': args.target_dims[0],
                    'win_ysize': args.target_dims[1],
                }
                array = band.ReadAsArray(**offset_dict)
                LOGGER.debug(f'{raster_path}: {offset_dict}')
                if nodata is None:
                    break
                nodata_count = numpy.count_nonzero(array == nodata)
                if nodata_count < 0.1*array.size:
                    # choose it
                    break

            target_path = os.path.join(
                args.target_dir,
                f'%s_{array.shape[1]}x{array.shape[0]}%s' % os.path.splitext(
                    basename))
            LOGGER.info(f'writing {target_path}')
            geoprocessing.numpy_array_to_raster(
                array, nodata, (1, -1), (0, 0),
                WORLD_ECKERT_IV_WKT, target_path)


if __name__ == '__main__':
    main()
