"""Test how fast we can read and write."""
import time

from ecoshard import geoprocessing


if __name__ == "__main__":
    path = r"C:\Users\richp\Downloads\impact_obs_10m_2017_compressed_md5_99376e.tif"
    start_time = time.time()
    for _ in geoprocessing.iterblocks((path, 1), offset_only=True):
        pass
    print(f'took {time.time()-start_time:.2f} seconds to iterblocks with only offset')
    for _ in geoprocessing.iterblocks((path, 1)):
        pass
    print(f'took {time.time()-start_time:.2f} seconds to iterblocks with read')
