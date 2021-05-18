
import pandas as pd
import math

def haversine(coord1, coord2):
    R = 6372800  # Earth radius in meters
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    
    phi1, phi2 = math.radians(lat1), math.radians(lat2) 
    dphi       = math.radians(lat2 - lat1)
    dlambda    = math.radians(lon2 - lon1)
    
    a = math.sin(dphi/2)**2 +         math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    
    return 2*R*math.atan2(math.sqrt(a), math.sqrt(1 - a))

def v_haversize(base, gps):
    import numpy as np
    lat1,lon1 = base
    lat2 = gps.iloc[:,0]
    lon2 = gps.iloc[:,1]
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    dphi= np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    R = 6372800  # Earth radius in meters
    a = np.sin(dphi/2)**2 + np.cos(phi1)*np.cos(phi2)*np.sin(dlambda/2)**2
    return 2* R * np.arctan2(np.sqrt(a), np.sqrt(1-a))


gps = pd.read_table('cold/gps-coords.tsv', index_col=0)
gps_d = pd.DataFrame({ix:v_haversize(gps.loc[ix], gps) for ix in gps.index})
gps_d.to_csv('tables/gps-dist.tsv', sep='\t')
