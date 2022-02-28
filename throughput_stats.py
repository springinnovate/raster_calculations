"""Test how fast we can read and write."""
import time

from ecoshard import geoprocessing


if __name__ == "__main__":
    path = r"C:\Users\richp\Downloads\impact_obs_10m_2017_compressed_md5_99376e.tif"
    for exp in reversed(range(20, 31)):
        start_time = time.time()
        largest_block = 2**exp
        for _ in geoprocessing.iterblocks(
                (path, 1), offset_only=True, largest_block=largest_block):
            pass
        print(f'offset,{time.time()-start_time:.2f},{exp}')
        start_time = time.time()
        for _ in geoprocessing.iterblocks(
                (path, 1), largest_block=largest_block):
            pass
        print(f'iterblocks,{time.time()-start_time:.2f},{exp}')
