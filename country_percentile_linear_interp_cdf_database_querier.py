"""Tracer script showing how to interact with CDF country database."""
import logging
import os
import pickle
import sys

from taskgraph.Task import _execute_sqlite
import numpy


WORK_DATABASE_PATH = 'work_status.db'
WORKSPACE_DIR = 'linear_cdf_workspace'
PERCENTILE_LIST = list(range(0, 101, 1))

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(processName)s %(levelname)s '
        '%(name)s [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
logging.getLogger('matplotlib').setLevel(logging.ERROR)
logging.getLogger('taskgraph').setLevel(logging.INFO)


def linear_interpolate_cdf(base_cdf):
    """Linear interpolate regions of straight lines in the CDF.

    Parameters:
        base_cdf (list): n elements of non-decreasing order.

    Returns:
        list of length base_cdf where consecutive elements of straight lines
        are linearly interpolated between the left and right sides.

    """
    target_cdf = list(base_cdf)
    index = 0
    left_val = 0
    while index < len(base_cdf)-1:
        if base_cdf[index] == base_cdf[index+1]:
            # search for where it ends
            offset = index+1
            while (offset < len(base_cdf)-1 and
                   base_cdf[offset] == base_cdf[offset+1]):
                offset += 1
            # linearly interpolate between index and offset
            right_val = base_cdf[offset]
            interp_val = numpy.interp(
                list(range(index, offset+1, 1)),
                [index-1, offset],
                [float(left_val), float(right_val)])
            target_cdf[index:offset+1] = interp_val
            left_val = right_val
            index = offset+1
        else:
            left_val = base_cdf[index]
            index += 1
    return target_cdf


def main():
    try:
        os.makedirs(WORKSPACE_DIR)
    except OSError:
        pass
    LOGGER.debug('building histogram/cdf')
    raster_id_list = _execute_sqlite(
        '''
        SELECT raster_id
        FROM job_status
        GROUP BY raster_id
        ''',
        WORK_DATABASE_PATH, execute='execute', argument_list=[],
        fetch='all')
    for (raster_id,) in raster_id_list:
        LOGGER.debug('building csv for %s', raster_id)
        result = _execute_sqlite(
            '''
            SELECT
              feature_id, percentile_list, percentile0_list, cdf, cdfnodata0
            FROM job_status
            WHERE raster_id=?
            ''',
            WORK_DATABASE_PATH, execute='execute', argument_list=[raster_id],
            fetch='all')

        percentile_map = {
            feature_id: (
                pickle.loads(percentile_list),
                pickle.loads(percentile0_list),
                linear_interpolate_cdf(pickle.loads(cdf)[::-1]),
                linear_interpolate_cdf(pickle.loads(cdfnodata0)[::-1]))
            for (feature_id, percentile_list, percentile0_list,
                 cdf, cdfnodata0) in result
            if None not in (
                feature_id, percentile_list, percentile0_list, cdf, cdfnodata0)
        }

        csv_percentile_path = os.path.join(
            WORKSPACE_DIR, '%s_percentile.csv' % raster_id)
        csv_nodata0_percentile_path = os.path.join(
            WORKSPACE_DIR, '%s_nodata0_percentile.csv' % raster_id)

        csv_cdf_path = os.path.join(
            WORKSPACE_DIR, '%s_cdf.csv' % raster_id)
        csv_nodata0_cdf_path = os.path.join(
            WORKSPACE_DIR, '%s_nodata0_cdf.csv' % raster_id)

        with open(csv_cdf_path, 'w') as csv_cdf_file:
            csv_cdf_file.write('%s cdfs' % raster_id)
            csv_cdf_file.write(
                '\ncountry,' +
                ','.join([str(x) for x in PERCENTILE_LIST]))
            # first do the whole world
            for feature_id in ['_GLOBAL'] + \
                    sorted(set(percentile_map)-set(['_GLOBAL'])):
                csv_cdf_file.write(
                    '\n%s,' % feature_id +
                    ','.join(([
                        str(x) for x in percentile_map[feature_id][2]])))

        with open(csv_nodata0_cdf_path, 'w') as csv_cdf_nodata0_file:
            csv_cdf_nodata0_file.write('%s cdfs' % raster_id)
            csv_cdf_nodata0_file.write(
                '\ncountry,' +
                ','.join([str(x) for x in PERCENTILE_LIST]))
            # first do the whole world
            for feature_id in ['_GLOBAL'] + \
                    sorted(set(percentile_map)-set(['_GLOBAL'])):
                csv_cdf_nodata0_file.write(
                    '\n%s,' % feature_id +
                    ','.join(([
                        str(x) for x in percentile_map[feature_id][3]])))

        with open(csv_percentile_path, 'w') as csv_percentile_file:
            csv_percentile_file.write('%s percentiles' % raster_id)
            csv_percentile_file.write(
                '\ncountry,' +
                ','.join([str(x) for x in PERCENTILE_LIST]))
            # first do the whole world
            for feature_id in ['_GLOBAL'] + \
                    sorted(set(percentile_map)-set(['_GLOBAL'])):
                csv_percentile_file.write(
                    '\n%s,' % feature_id +
                    ','.join([str(x) for x in percentile_map[feature_id][0]]))

        with open(csv_nodata0_percentile_path, 'w') as \
                csv_nodata0_percentile_file:
            csv_nodata0_percentile_file.write('%s percentiles' % raster_id)
            csv_nodata0_percentile_file.write(
                '\ncountry,' +
                ','.join([str(x) for x in PERCENTILE_LIST]))
            # first do the whole world
            for feature_id in ['_GLOBAL'] + \
                    sorted(set(percentile_map)-set(['_GLOBAL'])):
                csv_nodata0_percentile_file.write(
                    '\n%s,' % feature_id +
                    ','.join([str(x) for x in percentile_map[feature_id][1]]))


if __name__ == '__main__':
    main()
