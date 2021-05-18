import re
import pandas as pd
import numpy as np

def parse_lat(lat):
    try:
        return  float(lat)
    except:
        pass
    val, is_N = re.match('^([-.+\d]+) *([NS])?$', lat).groups()
    is_N = (is_N == 'N')
    return ((+1) if is_N else (-1)) * float(val)
    
    
def parse_long(long):
    # special case
    if long == '116. 3 E':
        long = '116.3 E'
       
    try:
        return  float(long)
    except:
        pass
    val, is_E = re.match('^([-.+\d]+) *([WE])?$', long).groups()
    is_E = (is_E == 'E')
    return ((+1) if is_E else (-1)) * float(val)
    
def parse_lat_lon(latlon):
    lat,ns, lon, ew = re.match('^([.\d]+) ([NS]) ([.\d]+) ([EW])$', latlon).groups()
   
    lat = float(lat)
    if ns == 'S':
        lat = -lat
    lon = float(lon)
    if ew == 'W':
        lon = -lon
    return [lat, lon]

def normalize_latlon(metadata):
    latitude_fixed = pd.to_numeric(metadata.latitude.map(lambda v: {'None': np.nan}.get(v,v))).dropna()
    longitude_fixed = pd.to_numeric(metadata.longitude.map(lambda v: {'None': np.nan}.get(v,v))).dropna()
    geo_lat = metadata['geographic location (latitude)'].dropna().map(parse_lat)
    geo_long = metadata['geographic location (longitude)'].dropna().map(parse_long)

    lat = pd.concat([latitude_fixed, geo_lat, metadata['Latitude Start'].dropna()])
    lon = pd.concat([longitude_fixed, geo_long, metadata['Longitude Start'].dropna()])

    lat_lon =metadata.lat_lon.map(lambda v: (v if v not in ['missing', 'none', 'not applicable'] else np.nan)).dropna()
    lat_lon2 = (lat.map(lambda ell: "{} {}".format(abs(ell), ("N" if ell>= 0 else "S"))) + " " + lon.map(lambda ell: "{} {}".format(abs(ell), ("E" if ell>= 0 else "W"))))

    metadata.drop(['longitude',
                        'geographic location (latitude and longitude)',
                        'latitude',
                        'lat_lon',
                        'geographic location (longitude)',
                        'geographic location (latitude)',

                        'Latitude Start',
                        'Longitude Start',
                        ],
                axis=1, inplace=1)
        

    metadata['lat_lon'] = pd.concat([lat_lon, lat_lon2])
