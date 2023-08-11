"""Process the WWF SIPA SDR and SWY scenarios on 7/26/2023 while BCK is out."""
import sys
import time
import os
import logging

from ecoshard import taskgraph
from osgeo import gdal
import raster_calculations_core

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)

LOGGER = logging.getLogger(__name__)


WORKSPACE_DIR = f'workspace_{os.path.basename(os.path.splitext(__file__)[0])}'
os.makedirs(WORKSPACE_DIR, exist_ok=True)


def _make_logger_callback(message, timeout=5.0):
    """Build a timed logger callback that prints ``message`` replaced.

    Args:
        message (string): a string that expects 2 placement %% variables,
            first for % complete from ``df_complete``, second from
            ``p_progress_arg[0]``.
        timeout (float): number of seconds to wait until print

    Returns:
        Function with signature:
            logger_callback(df_complete, psz_message, p_progress_arg)

    """
    def logger_callback(df_complete, _, p_progress_arg):
        """Argument names come from the GDAL API for callbacks."""
        current_time = time.time()
        if ((current_time - logger_callback.last_time) > timeout or
                (df_complete == 1.0 and
                 logger_callback.total_time >= timeout)):
            # In some multiprocess applications I was encountering a
            # ``p_progress_arg`` of None. This is unexpected and I suspect
            # was an issue for some kind of GDAL race condition. So I'm
            # guarding against it here and reporting an appropriate log
            # if it occurs.
            progress_arg = ''
            if p_progress_arg is not None:
                progress_arg = p_progress_arg[0]

            LOGGER.info(message, df_complete * 100, progress_arg)
            logger_callback.last_time = current_time
            logger_callback.total_time += current_time
    logger_callback.last_time = time.time()
    logger_callback.total_time = 0.0

    return logger_callback


def cog_file(file_path, target_dir):
    # create copy with COG
    cog_driver = gdal.GetDriverByName('COG')
    base_raster = gdal.OpenEx(file_path, gdal.OF_RASTER)
    cog_file_path = os.path.join(
        WORKSPACE_DIR, f'cog_{os.path.basename(file_path)}')
    options = ('COMPRESS=LZW', 'NUM_THREADS=ALL_CPUS', 'BIGTIFF=YES')
    LOGGER.info(f'convert {file_path} to COG {cog_file_path} with {options}')
    cog_raster = cog_driver.CreateCopy(
        cog_file_path, base_raster, options=options,
        callback=_make_logger_callback(
            f"COGing {cog_file_path} %.1f%% complete %s"))
    del cog_raster


def main():
    files_to_cog_now = []
    files_to_cog_after = []
    for baseline_path, scenario_path, target_prefix in zip(
            [#r"D:\repositories\ndr_sdr_global\wwf_IDN_baseline_contc\stitched_sed_export_wwf_IDN_baseline_contc.tif",
             #r"D:\repositories\ndr_sdr_global\wwf_PH_baseline_contc\stitched_sed_export_wwf_PH_baseline_contc.tif",
             r"D:\repositories\swy_global\workspace_swy_wwf_ph_baseline_contCN\B_sum_wwf_ph_baseline_contCN.tif",
             #r"D:\repositories\swy_global\workspace_swy_wwf_ph_baseline_contCN\QF_wwf_ph_baseline_contCN_V2.tif",
             #r"D:\repositories\swy_global\workspace_swy_wwf_idn_baseline2\B_sum_wwf_idn_baseline2.tif",
             #r"D:\repositories\swy_global\workspace_swy_wwf_idn_baseline2\QF_wwf_idn_baseline2.tif",
             ],
            [#r"D:\repositories\ndr_sdr_global\wwf_PH_restoration\stitched_sed_export_wwf_PH_restoration.tif",
             #r"D:\repositories\ndr_sdr_global\wwf_IDN_restoration\stitched_sed_export_wwf_IDN_restoration.tif",
             r"D:\repositories\swy_global\workspace_swy_wwf_ph_restoration\B_sum_wwf_ph_restoration.tif",
             #r"D:\repositories\swy_global\workspace_swy_wwf_ph_restoration\QF_wwf_ph_restoration.tif",
             #r"D:\repositories\swy_global\workspace_swy_wwf_idn_restoration\B_sum_wwf_idn_restoration.tif",
             #r"D:\repositories\swy_global\workspace_swy_wwf_idn_restoration\QF_wwf_idn_restoration.tif",
             ],
            [#'sed_export_IDN_restoration-baseline_contc.tif',
             #'sed_export_PH_restoration-baseline_contc.tif',
             'B_sum_PH_restoration-_baseline_contCN',
             #'QF_PH_restoration-_baseline_contCN_V2',
             #'B_sum_IDN_restoration-baseline2',
             #'QF_IDN_restoration-baseline2',
             ]):
        if not os.path.exists(baseline_path) or not os.path.exists(scenario_path):
            raise ValueError(f'missing {baseline_path} or {scenario_path}')

        files_to_cog_now.append(baseline_path)
        files_to_cog_now.append(scenario_path)
        target_raster_path = os.path.join(
            WORKSPACE_DIR, f'{target_prefix}.tif')
        single_expression = {
            'expression': 'scenario_path-baseline_path',
            'symbol_to_path_map': {
                'scenario_path': scenario_path,
                'baseline_path': baseline_path,
            },
            'target_nodata': -1,
            'target_raster_path': target_raster_path,
        }

        raster_calculations_core.evaluate_calculation(
            single_expression, TASK_GRAPH, WORKSPACE_DIR)
        files_to_cog_after.append(target_raster_path)

    for file_path in files_to_cog_now:
        TASK_GRAPH.add_task(
            func=cog_file, args=(file_path, WORKSPACE_DIR),
            task_name=f'cog {file_path}')

    TASK_GRAPH.join()

    for file_path in files_to_cog_after:
        TASK_GRAPH.add_task(
            func=cog_file, args=(file_path, WORKSPACE_DIR),
            task_name=f'cog {file_path}')

    TASK_GRAPH.join()
    TASK_GRAPH.close()


if __name__ == '__main__':
    TASK_GRAPH = taskgraph.TaskGraph(WORKSPACE_DIR, 6, 15.0)
    main()
