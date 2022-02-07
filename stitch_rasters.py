"""Script to stitch arbitrary rasters together."""
import argparse
import ecoshard
import glob
import itertools
import logging
import math
import os

from osgeo import gdal
from osgeo import osr
from ecoshard import geoprocessing


logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.DEBUG)
gdal.SetCacheMax(2**26)


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(
        description=(
            'Search for matching rasters to stitch into one big '
            'raster.'))
    parser.add_argument(
        '--target_projection_epsg', required=True,
        help='EPSG code of target projection')
    parser.add_argument(
        '--target_cell_size', required=True,
        help=(
            'A single float indicating the desired square pixel size of '
            'the stitched raster.'))
    parser.add_argument(
        '--resample_method', default='near', help=(
            'One of near|bilinear|cubic|cubicspline|lanczos|average|mode|max'
            'min|med|q1|q3'))
    parser.add_argument(
        '--target_raster_path', required=True, help='Path to target raster.')
    parser.add_argument(
        '--raster_list', nargs='+',
        help='List of rasters or wildcards to stitch.')
    parser.add_argument(
        '--raster_pattern', nargs=2, help=(
            'Recursive directory search for raster pattern such that '
            'the first argument is the directory to search and the second '
            'is the filename pattern.'))
    parser.add_argument(
        '--overlap_algorithm', default='replace', help=(
            'can be one of etch|replace|add, default is replace'))
    parser.add_argument(
        '--_n_limit', type=int,
        help=(
            'limit the number of stitches to this number, default is to '
            'stitch all found rasters'))

    parser.add_argument(
        '--area_weight_m2_to_wgs84', action='store_true',
        help=(
            'if true, rescales values to be proportional to area change '
            'for wgs84 coordinates'))

    parser.add_argument(
        '--workspace_dir', help=(
            'temporary directory used for warping files, useful for '
            'avoiding rewarping of files, otherwise defaults to '
            'stitch_workspace_[target file name]'))

    args = parser.parse_args()

    if args.workspace_dir is None:
        args.workspace_dir = f"""stitch_raster_workspace_{
            os.path.basename(os.path.splitext(
                args.target_raster_path)[0])}"""

    if not args.raster_list != args.raster_pattern:
        raise ValueError(
            'only one of --raster_list or --raster_pattern must be '
            'specified: \n'
            f'args.raster_list={args.raster_list}\n'
            f'args.raster_pattern={args.raster_pattern}\n')

    LOGGER.info('searching for matching files')
    if args.raster_list:
        raster_path_list = list(
            raster_path for raster_glob in args.raster_list
            for raster_path in glob.glob(raster_glob)
            )
    else:
        base_dir = args.raster_pattern[0]
        file_pattern = args.raster_pattern[1]
        LOGGER.info(f'searching {base_dir} for {file_pattern}')

        raster_path_list = list(itertools.islice(
            (raster_path for walk_info in os.walk(base_dir)
             for raster_path in glob.glob(os.path.join(
                walk_info[0], file_pattern))), 0, args._n_limit))
        LOGGER.info(f'found {len(raster_path_list)} files that matched')

    target_projection = osr.SpatialReference()
    target_projection.ImportFromEPSG(int(args.target_projection_epsg))

    if len(raster_path_list) == 0:
        raise RuntimeError(
            f'no rasters were found with the pattern "{file_pattern}"')

    LOGGER.info('calculating target bounding box')
    target_bounding_box_list = []
    raster_path_set = set()
    for raster_path in raster_path_list:
        if raster_path in raster_path_set:
            LOGGER.warning(f'{raster_path} already scheduled')
            continue
        raster_path_set.add(raster_path)
        raster_info = geoprocessing.get_raster_info(raster_path)
        bounding_box = raster_info['bounding_box']
        target_bounding_box = geoprocessing.transform_bounding_box(
            bounding_box, raster_info['projection_wkt'],
            target_projection.ExportToWkt())
        target_bounding_box_list.append(target_bounding_box)

    target_bounding_box = geoprocessing.merge_bounding_box_list(
        target_bounding_box_list, 'union')

    gtiff_driver = gdal.GetDriverByName('GTiff')

    n_cols = int(math.ceil(
        (target_bounding_box[2]-target_bounding_box[0]) /
        float(args.target_cell_size)))
    n_rows = int(math.ceil(
        (target_bounding_box[3]-target_bounding_box[1]) /
        float(args.target_cell_size)))

    geotransform = (
        target_bounding_box[0], float(args.target_cell_size), 0.0,
        target_bounding_box[3], 0.0, -float(args.target_cell_size))

    target_raster = gtiff_driver.Create(
        os.path.join('.', args.target_raster_path), n_cols, n_rows, 1,
        raster_info['datatype'],
        options=(
            'TILED=YES', 'BIGTIFF=YES', 'BLOCKXSIZE=256', 'BLOCKYSIZE=256',
            'COMPRESS=ZSTD', 'SPARSE_OK=TRUE'))
    target_raster.SetProjection(target_projection.ExportToWkt())
    target_raster.SetGeoTransform(geotransform)
    target_band = target_raster.GetRasterBand(1)
    target_nodata = raster_info['nodata'][0]
    if target_nodata is not None:
        target_band.SetNoDataValue(target_nodata)
    else:
        target_band.SetNoDataValue(-9999)
    target_band = None
    target_raster = None

    LOGGER.info('calling stitch_rasters')
    geoprocessing.stitch_rasters(
        [(path, 1) for path in raster_path_list],
        [args.resample_method]*len(raster_path_list),
        (args.target_raster_path, 1),
        overlap_algorithm=args.overlap_algorithm,
        run_parallel=True,
        working_dir=args.workspace_dir,
        area_weight_m2_to_wgs84=args.area_weight_m2_to_wgs84)

    LOGGER.debug('build overviews...')
    ecoshard.build_overviews(args.target_raster_path)
    LOGGER.info('all done')


if __name__ == '__main__':
    main()
