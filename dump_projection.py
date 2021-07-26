"""Dump EPSG code to stdout."""
import argparse

from osgeo import osr

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Dump epsg wtk')
    parser.add_argument('epsg_code', type=int, help='EPSG code')
    args = parser.parse_args()
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(int(args.epsg_code))
    print(srs.ExportToWkt())
