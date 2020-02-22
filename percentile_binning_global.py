"""Mask global rasters to have integer bins rather than values."""
import glob
import os
import re

import pandas

if __name__ == '__main__':
    for raster_path in glob.glob('realized_service/*.tif'):
        dirname = os.path.dirname(raster_path)
        basename = os.path.basename(raster_path)
        base_without_hash = re.match('(.*)_md5*', basename).group(1)
        print(dirname, base_without_hash)
        for table_path in glob.glob(os.path.join(
            dirname, 'tables', '%s*.csv' % os.path.splitext(
                base_without_hash)[0])):
            frame = pandas.read_csv(table_path).iloc[0]
            print(frame)
