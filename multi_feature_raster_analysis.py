"""Multi field polygon raster analysis for Rob."""
import logging
import os
import multiprocessing
import sys

from osgeo import gdal
import pygeoprocessing
import numpy
import taskgraph

gdal.SetCacheMax(2**28)

WORKSPACE_DIR = 'workspace'
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.INFO)


def rasterize_like(
        base_raster_path, base_vector_path, fieldname,
        target_raster_type, target_nodata, target_raster_path):
    """Rasterize `fieldname` to a new raster same size as `base_raster_path`"""
    pygeoprocessing.new_raster_from_base(
        base_raster_path, target_raster_path, target_raster_type,
        [target_nodata])
    pygeoprocessing.rasterize(
        base_vector_path, target_raster_path,
        option_list=['ATTRIBUTE=%s' % fieldname])


def robs_function(
        base_array, RES1, RES2, RES3A, RES3B, RES3C, RES3D, RES3E, RES3F,
        RES4, RES5, RES6, COM1, COM2, COM3, COM4, COM5, COM6,
        COM7, COM8, COM9, COM10, IND1, IND2, IND3, IND4,
        IND5, IND6, AGR1, REL1, GOV1, GOV2, EDU1, EDU2, nodata):
    """Rob you should write your function here."""
    result = numpy.empty_like(base_array)
    result[:] = nodata
    valid_mask = (
        ~numpy.isclose(base_array, nodata) &
        ~numpy.isclose(RES1, nodata) &
        ~numpy.isclose(RES2, nodata) &
        ~numpy.isclose(RES3A, nodata) &
        ~numpy.isclose(RES3B, nodata) &
        ~numpy.isclose(RES3C, nodata) &
        ~numpy.isclose(RES3D, nodata) &
        ~numpy.isclose(RES3E, nodata) &
        ~numpy.isclose(RES3F, nodata) &
        ~numpy.isclose(RES4, nodata) &
        ~numpy.isclose(RES5, nodata) &
        ~numpy.isclose(RES6, nodata) &
        ~numpy.isclose(COM1, nodata) &
        ~numpy.isclose(COM2, nodata) &
        ~numpy.isclose(COM3, nodata) &
        ~numpy.isclose(COM4, nodata) &
        ~numpy.isclose(COM5, nodata) &
        ~numpy.isclose(COM6, nodata) &
        ~numpy.isclose(COM7, nodata) &
        ~numpy.isclose(COM8, nodata) &
        ~numpy.isclose(COM9, nodata) &
        ~numpy.isclose(COM10, nodata) &
        ~numpy.isclose(IND1, nodata) &
        ~numpy.isclose(IND2, nodata) &
        ~numpy.isclose(IND3, nodata) &
        ~numpy.isclose(IND4, nodata) &
        ~numpy.isclose(IND5, nodata) &
        ~numpy.isclose(IND6, nodata) &
        ~numpy.isclose(AGR1, nodata) &
        ~numpy.isclose(REL1, nodata) &
        ~numpy.isclose(GOV1, nodata) &
        ~numpy.isclose(GOV2, nodata) &
        ~numpy.isclose(EDU1, nodata) &
        ~numpy.isclose(EDU2, nodata))

    result[valid_mask] = (
        base_array[valid_mask] * RES1[valid_mask] * RES2[valid_mask] *
        RES3A[valid_mask] * RES3B[valid_mask] * RES3C[valid_mask] *
        RES3D[valid_mask] * RES3E[valid_mask] * RES3F[valid_mask] *
        RES4[valid_mask] * RES5[valid_mask] * RES6[valid_mask] *
        COM1[valid_mask] * COM2[valid_mask] * COM3[valid_mask] *
        COM4[valid_mask] * COM5[valid_mask] * COM6[valid_mask] *
        COM7[valid_mask] * COM8[valid_mask] * COM9[valid_mask] *
        COM10[valid_mask] * IND1[valid_mask] * IND2[valid_mask] *
        IND3[valid_mask] * IND4[valid_mask] * IND5[valid_mask] *
        IND6[valid_mask] * AGR1[valid_mask] * REL1[valid_mask] *
        GOV1[valid_mask] * GOV2[valid_mask] * EDU1[valid_mask] *
        EDU2[valid_mask])
    return result


if __name__ == "__main__":
    for dir_path in [WORKSPACE_DIR, CHURN_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    task_graph = taskgraph.TaskGraph(
        CHURN_DIR, multiprocessing.cpu_count(), 5.0)

    vector_path = 'data/Exposure4OLU2.shp'
    raster_path = 'data/olu_00000-0-0-000-000000-00000-00-00000-00_depth.tif'
    raster_info = pygeoprocessing.get_raster_info(raster_path)
    raster_projection = raster_info['projection']
    base_nodata = raster_info['nodata'][0]

    reprojected_vector_path = os.path.join(
        CHURN_DIR, 'reprojected_' + os.path.basename(vector_path))

    reproject_task = task_graph.add_task(
        func=pygeoprocessing.reproject_vector,
        args=(vector_path, raster_projection, reprojected_vector_path),
        target_path_list=[reprojected_vector_path],
        task_name='project vector to raster')

    fields_to_process = [
        'RES1', 'RES2', 'RES3A', 'RES3B', 'RES3C', 'RES3D', 'RES3E', 'RES3F',
        'RES4', 'RES5', 'RES6', 'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6',
        'COM7', 'COM8', 'COM9', 'COM10', 'IND1', 'IND2', 'IND3', 'IND4',
        'IND5', 'IND6', 'AGR1', 'REL1', 'GOV1', 'GOV2', 'EDU1', 'EDU2']

    # wait for vector to reproject
    # reproject_task.join()
    # vector = gdal.OpenEx(reprojected_vector_path, gdal.OF_VECTOR)
    # layer = vector.GetLayer()
    # layer_def = layer.GetLayerDefn()
    # field_name_list = [
    #     layer_def.GetFieldDefn(i).GetName()
    #     for i in range(layer_def.GetFieldCount())]
    # layer_def = None
    # layer = None
    # vector = None

    # this rasterizes each field into a unique raster and puts it in a list
    # for the raster calculator later
    rasterize_task_list = []
    raster_path_band_list = [(raster_path, 1)]
    for field_name in fields_to_process:
        target_raster_path = os.path.join(
            CHURN_DIR, '%s.tif' % field_name if 'COM' not in field_name else '_' + field_name)
        rasterize_task = task_graph.add_task(
            func=rasterize_like,
            args=(
                raster_path, reprojected_vector_path, field_name,
                raster_info['datatype'], base_nodata,
                target_raster_path),
            ignore_path_list=[target_raster_path],
            dependent_task_list=[reproject_task],
            target_path_list=[target_raster_path],
            task_name='rasterize %s' % field_name)
        rasterize_task_list.append(rasterize_task)
        raster_path_band_list.append((target_raster_path, 1))

    # this calculates the function after the vector is rasterized
    robs_function_raster_path = os.path.join(
        WORKSPACE_DIR, 'this_is_the_output_function.tif')
    task_graph.add_task(
        func=pygeoprocessing.raster_calculator,
        args=(
            raster_path_band_list + [(base_nodata, 'raw')],
            robs_function, robs_function_raster_path, raster_info['datatype'],
            base_nodata),
        dependent_task_list=rasterize_task_list,
        target_path_list=[robs_function_raster_path],
        task_name='this is the final function calculation')

    task_graph.close()
    task_graph.join()
