import logging
import numpy
from ecoshard import taskgraph
from ecoshard import geoprocessing
import raster_calculations_core
from osgeo import gdal

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(__name__)
logging.getLogger('ecoshard.taskgraph').setLevel(logging.WARN)

if __name__ == '__main__':
    base_byte_array = numpy.array([[51]], dtype=numpy.byte)
    base_byte_path = 'byte.tif'
    geoprocessing.numpy_array_to_raster(
        base_byte_array, None, (1, -1), (0, 0), None,
        base_byte_path)

    base_float_array = numpy.array([[100]], dtype=numpy.byte)
    base_float_path = 'float.tif'
    geoprocessing.numpy_array_to_raster(
        base_float_array, None, (1, -1), (0, 0), None,
        base_float_path)

    calculation = {
        'expression': 'raster1 * raster2',
        'symbol_to_path_map': {
            'raster1': base_byte_path,
            'raster2': base_float_path,
        },
        'target_datatype': gdal.GDT_Float32,
        'target_raster_path': 'out.tif',
        'target_nodata': -1,
    }
    task_graph = taskgraph.TaskGraph('.', -1)
    raster_calculations_core.evaluate_calculation(
            calculation, task_graph, '.')

    LOGGER.debug(geoprocessing.raster_to_numpy_array('out.tif'))
