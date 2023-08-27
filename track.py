from datetime import datetime

import numpy as np


class Track(dict):
    def __init__(self,times:list[datetime]=None,lats_deg:list[float]=None,lats_rad:list[float]=None,
                                                lons_deg:list[float]=None,lons_rad:list[float]=None):
        super().__init__()
        if times is not None:
            if lats_rad is None:
                lats_rad=[np.deg2rad(lat_deg) for lat_deg in lats_deg]
            if lons_rad is None:
                lons_rad=[np.deg2rad(lon_deg) for lon_deg in lons_deg]
            for time,lat_rad,lon_rad in zip(times,lats_rad,lons_rad):
                self[time]=(lat_rad,lon_rad)
