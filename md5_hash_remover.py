"""For renaming all those super long files!"""
import argparse
import glob
import logging
import os
import re
import sys

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format=('%(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Rename hashed files.')
    parser.add_argument(
        'filepath', nargs='+', help='Files/patterns to search and rename.')
    parser.add_argument('--hash_function', default='md5')
    args = parser.parse_args()
    for glob_pattern in args.filepath:
        for file_path in glob.glob(glob_pattern):
            LOGGER.info('processing %s', file_path)
            prefix, suffix = os.path.splitext(file_path)
            file_match = re.match(
                '(.*?)_%s_.*$' % (args.hash_function), prefix)
            if file_match:
                rename_file_path = '%s%s' % (file_match.group(1), suffix)
                LOGGER.info(
                    'matched %s, renaming to %s', file_path, rename_file_path)
                os.rename(file_path, rename_file_path)
