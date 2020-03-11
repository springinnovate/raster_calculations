"""Tracer script showing how to interact with CDF country database."""
import bisect
import pickle

from taskgraph.Task import _execute_sqlite


WORK_DATABASE_PATH = 'work_status.db'

PERCENTILE_LIST = list(range(0, 101, 1))


def main():
    result = _execute_sqlite(
        '''
        SELECT
          percentile_list, percentile0_list, cdf, cdfnodata0
        FROM job_status
        WHERE raster_id=? AND country_id=?;
        ''',
        WORK_DATABASE_PATH, execute='execute', argument_list=[
            'realized_grazing_natnotforest', 'CHN'],
        fetch='one')

    for index, list_id in enumerate(
            ['percentile_list', 'percentile0_list', 'cdf', 'cdfnodata0']):
        print('%s: %s' % (list_id, pickle.loads(result[index])))

    cdf_list = pickle.loads(result[3])
    percent_of_total_value = [x/cdf_list[0] for x in cdf_list]
    print('%s:\n%s' % ('cdf', '\n'.join([
        '%3.d %10.2f %5.2f' % (percentile, cdf, percent_of_total_value)
        for percentile, cdf, percent_of_total_value in
        zip(PERCENTILE_LIST, cdf_list, percent_of_total_value)])))

    cdf_threshold_value = .9
    threshold_index = bisect.bisect_left(
        percent_of_total_value[::-1], cdf_threshold_value)
    print('threshold val:\n%s: %s' % (
        cdf_threshold_value, cdf_list[len(cdf_list)-threshold_index]))


if __name__ == '__main__':
    main()
