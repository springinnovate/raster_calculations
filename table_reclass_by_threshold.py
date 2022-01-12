"""Table based reclassify triggered by probability threshold."""
import argparse

from ecoshard import geoprocessing
import pandas




def main():
    """Entry point."""
    parser = argparse.ArgumentParser(
        description='reclassify raster to table based on probability threshold')
    parser.add_argument(
        '--base_raster_path', type=str, required=True,
        help='path to integer raster')
    parser.add_argument(
        '--threshold_raster_path', type=str, required=True,
        help='path to threshold raster')
    parser.add_argument(
        '--threshold_value', type=float, required=True, help=(
            'floating point value, if threshold raster is greater than this '
            'value, reclassify based on > column of table.value in 0..1 to '
            'flip lulc pixel'))
    parser.add_argument(
        '--reclassify_table_path', type=str, required=True, help=(
            'path to csv table with columns'))
    parser.add_argument(
        '--csv_table_fields', type=str, nargs=3, required=True, help=(
            'column names for base raster value, value to flip to if <= '
            'threshold, and value to flip to if > threshold'))
    parser.add_argument(
        '--target_raster_path', type=str,
        help='desired target raster')
    args = parser.parse_args()
    print(args.csv_table_fields)
    df = pandas.read_csv(args.reclassify_table_path)
    value_map = {
        int(base_lucode): (float(leq_target), float(gt_target))
        for (base_lucode, leq_target, gt_target) in zip(
            df[args.csv_table_fields[0]],
            df[args.csv_table_fields[1]],
            df[args.csv_table_fields[2]])
        }
    print(value_map)

    def _reclass_op(base_array, threshold_array):
        result = base_array.copy()
        for base_code, (leq_target, gt_target) in value_map.items():
            leq_mask = (
                base_array == base_code) * (threshold_array <= leq_target)
            result[leq_mask] = leq_target

            gt_mask = (
                base_array == base_code) * (threshold_array > gt_target)
            result[gt_mask] = gt_target
        return result

    base_raster_info = geoprocessing.get_raster_info(args.base_raster_path)
    geoprocessing.raster_calculator(
        [(args.base_raster_path, 1), (args.threshold_raster_path, 1)],
        _reclass_op, args.target_raster_path, base_raster_info['datatype'],
        base_raster_info['nodata'][0])


if __name__ == '__main__':
    main()
