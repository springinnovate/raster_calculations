"""Mask global rasters to have integer bins rather than values."""
import glob
import os
import re

import pandas

PERCENTILES = [
    0, 0.01, 1, 2, 3, 4, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70,
    75, 80, 85, 90, 95, 96, 97, 98, 99, 99.9, 100]

TENTH_INDEXES = [PERCENTILES.index(x) for x in range(10, 101, 10)]

BIN_INTEGERS = [1 + int(x) // 10 for x in PERCENTILES]
print(TENTH_INDEXES)

if __name__ == '__main__':
    for raster_path in glob.glob('realized_service/*.tif'):
        dirname = os.path.dirname(raster_path)
        basename = os.path.basename(raster_path)
        base_without_hash = re.match('(.*)_md5*', basename).group(1)
        print(dirname, base_without_hash)
        for table_path in glob.glob(os.path.join(
            dirname, 'tables', '%s*.csv' % os.path.splitext(
                base_without_hash)[0])):
            frame = pandas.read_csv(table_path)
            cutoff_percentile_values = []
            for index in TENTH_INDEXES:
                cutoff_percentile_values.append(frame.iloc[0, 2+index])
        print(cutoff_percentile_values)
