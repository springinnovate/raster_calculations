"""Generate a subset of polys where raster mask is defined."""
import argparse
import glob
import logging
import os
import multiprocessing

from ecoshard import geoprocessing
from ecoshard import taskgraph
from osgeo import gdal
import numpy

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(__name__)
logging.getLogger('ecoshard.taskgraph').setLevel(logging.WARN)
gdal.SetCacheMax(2**26)


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(
        description=(
            'Generate subset of polys where raster mask is defined.'))
    parser.add_argument(
        '--base_poly_list', nargs='+', required=True,
        help='Path(s)/pattern(s) to poly vectors.')
    parser.add_argument(
        '--mask_raster_path', required=True,
        help='Path to mask raster')
    parser.add_argument(
        '--mask_id', type=int, default=1,
        help='Mask value to use for selection.')
    args = parser.parse_args()

    poly_path_list = [
        path for pattern in args.base_poly_list
        for path in glob.glob(pattern)]

    workspace_dir = 'polys_by_mask_workspace'
    task_graph = taskgraph.TaskGraph(
        workspace_dir, multiprocessing.cpu_count(), 15.0)

    for poly_path in poly_path_list:
        poly_base = os.path.basename(os.path.splitext(poly_path)[0])
        poly_workspace = os.path.join(workspace_dir, poly_base)
        os.makedirs(poly_workspace, exist_ok=True)
        poly_vector = gdal.OpenEx(poly_path, gdal.OF_VECTOR)
        poly_layer = poly_vector.GetLayer()
        poly_in_mask_list = []
        for poly_feature in poly_layer:
            fid = poly_feature.GetFID()
            fid = 22476
            poly_in_mask_task = task_graph.add_task(
                func=poly_in_mask,
                args=(
                    fid, poly_path, args.mask_raster_path, poly_workspace,
                    args.mask_id),
                store_result=True,
                task_name=f'test {poly_path}_{fid}')
            poly_in_mask_list.append((fid, poly_in_mask_task))
            break
        valid_fid_list = [
            fid for (fid, task) in poly_in_mask_list if task.get()]
        LOGGER.debug(valid_fid_list)


def poly_in_mask(
        fid, poly_path, mask_raster_path, workspace_dir, mask_id):
    poly_vector = gdal.OpenEx(poly_path, gdal.OF_VECTOR)
    poly_layer = poly_vector.GetLayer()
    poly_feature = poly_layer.GetFeature(fid)
    poly_geom = poly_feature.GetGeometryRef()
    poly_envelope = poly_geom.GetEnvelope()
    poly_bb = [poly_envelope[i] for i in [0, 2, 1, 3]]
    poly_geom = None
    poly_feature = None
    poly_layer = None
    poly_vector = None

    clipped_raster_path = os.path.join(
        workspace_dir, f'{fid}', f'clipped_{fid}_mask.tif')
    os.makedirs(os.path.dirname(clipped_raster_path), exist_ok=True)

    mask_raster_info = geoprocessing.get_raster_info(mask_raster_path)
    poly_info = geoprocessing.get_vector_info(poly_path)

    target_poly_bb = geoprocessing.transform_bounding_box(
        poly_bb,
        poly_info['projection_wkt'],
        mask_raster_info['projection_wkt'])

    try:
        geoprocessing.merge_bounding_box_list(
            [target_poly_bb, mask_raster_info['bounding_box']],
            'intersection')
    except ValueError:
        LOGGER.info(f'{fid} not in {mask_raster_path}')
        return False

    geoprocessing.warp_raster(
        mask_raster_path,
        mask_raster_info['pixel_size'], clipped_raster_path,
        'near', target_poly_bb,
        target_projection_wkt=mask_raster_info['projection_wkt'],
        vector_mask_options={
            'mask_vector_path': poly_path,
            'mask_vector_where_filter': f'"FID" in ({fid})',
            })
    for _, block in geoprocessing.iterblocks((clipped_raster_path, 1)):
        if numpy.any(block == mask_id):
            return True
    return False


if __name__ == '__main__':
    main()
