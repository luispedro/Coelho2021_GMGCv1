import re
import pandas as pd
import numpy as np
def parse_lat_lon(val):
    m = re.match(r'(\d+)°(\d+)’(\d+)” ([NS]), (\d+)°(\d+)’(\d+)” ?([WE])', val)
    if m:
        lat1,lat2,lat3,latdir,lon1,lon2,lon3,londir = m.groups()
        lat = int(lat1)+int(lat2)/60. + int(lat3)/3600.
        if latdir == 'S':
            lat *= -1
        lon = int(lon1)+int(lon2)/60. + int(lon3)/3600.
        if londir == 'E':
            lon *= -1
        return pd.Series({'latitude': lat, 'longitude': lon})
    m = re.match(r'^(\d+\.\d+) ?([NS]),? *(-?\d+\.\d+) ?([WE])$', val)
    if m:
        lat,latdir,lon,londir = m.groups()
        lat = float(lat)
        if latdir == 'S':
            lat = -lat

        lon = float(lon)
        if londir == 'E':
            lon = -lon

        return pd.Series({'latitude': lat, 'longitude': lon})
    m = re.match(r'^(\d+\.\d+), *(-?\d+\.\d+)$', val)
    if m:
        lat,lon = m.groups()
        lat = float(lat)
        lon = float(lon)
        return pd.Series({'latitude': lat, 'longitude': lon})
    m = re.match(r'^(\d+)\'W, 37N', val)
    if m:
        lat = 37
        lon = float(m.group(1))
        return pd.Series({'latitude': lat, 'longitude': lon})
    raise ValueError(f'Could not parse {val}')

compl = pd.read_table('compl_geo_cntr.tsv', index_col=0)
biome = pd.read_table('biome.txt', index_col=0, squeeze=True)
meta = pd.read_table('/g/scb2/bork/coelho/DD_DeCaF/genecats.cold/selected-cleaned-metadata-100.tsv', index_col=0)
fullmeta = pd.read_table('/home/luispedro/work/genecats/sources/fetch-data/data/sample-meta.tsv', index_col=0)
unexplained = set(biome.index) - set(compl.index)
fullmeta_unexplained = fullmeta.loc[[e for e in unexplained if e in fullmeta.index]]

extra = fullmeta_unexplained[['geographic location (latitude)', 'geographic location (longitude)']].dropna()
extra.rename(columns={'geographic location (latitude)': 'latitude',
                      'geographic location (longitude)': 'longitude'}, inplace=True)

WILMES_LAT_LON = pd.Series({'latitude': 49.6116, 'longitude': -6.1319})

wilmes_lat_lon = pd.DataFrame({s:WILMES_LAT_LON for s in
 {'SRX1670018',
 'SRX1670019',
 'SRX1670020',
 'SRX1670022',
 'SRX1670023',
 'SRX1670024',
 'SRX1670025',
 'SRX1670026',
 'SRX1670027',
 'SRX1670028',
 'SRX1670029',
 'SRX1670030',
 'SRX1670031',
 'SRX1670032',
 'SRX1670033',
 'SRX1670034',
 'SRX1670035',
 'SRX1670036',
 'SRX1670037',
 'SRX1670038',
 'SRX1670039',
 'SRX1670040',
 'SRX1670041',
 'SRX1670042',
 'SRX1670043',
 'SRX1670044',
 'SRX1670045',
 'SRX1670046',
 'SRX1670048',
 'SRX1670049',
 'SRX1670051',
 'SRX1670052',
 'SRX1670053',
 'SRX1670054',
 'SRX1670055',
 'SRX1670056',
 'SRX1670057',
 'SRX1670058',
 'SRX1670059',
 'SRX1670060',
 'SRX1670062',
 'SRX1670063',
 'SRX1670064',
 'SRX1670065',
 'SRX1670066',
 'SRX1670068',
 'SRX1670069',
 'SRX1670070',
 'SRX1670071',
 'SRX1670072',
 'SRX1670073',
 'SRX1670074',
 'SRX1670075',
 'SRX1670076',
 'SRX1670077',
 'SRX1670079',
 'SRX1670080',
 'SRX1670081',
 'SRX1670083',
 'SRX1670084',
 'SRX1670085',
 'SRX1670086',
 'SRX1670087',
 'SRX1670088',
 'SRX1670089',
 'SRX1670091',
 'SRX1670092',
 'SRX1670093',
 'SRX1670094',
 'SRX1670095',
 'SRX1670096',
 'SRX1670097',
 'SRX1670098',
 'SRX1670099',
 'SRX1670100',
 'SRX1670101',
 'SRX1670102',
 'SRX1670103',
 'SRX1670104',
 'SRX1670105',
 'SRX1670106'}}).T

extra = extra.append(wilmes_lat_lon)
DOG_LOC = pd.Series({'latitude': 38.6270, 'longitude': 90.1994})
extra = extra.append(pd.DataFrame({s:DOG_LOC for s in unexplained if s.startswith('ExDog')}).T)
lat_lon = fullmeta_unexplained['lat_lon'].dropna()
extra = extra.append(lat_lon.apply(parse_lat_lon))
gmgc = pd.read_table('//home/luispedro/work/ena-meta/gmgc-metadata.tsv', index_col=0)
gmgc['lat_lon'] = gmgc['lat_lon'].replace('missing', np.nan)
gmgc_lat_lon = gmgc['lat_lon'].dropna()
extra = extra.append(gmgc_lat_lon.replace('55.453 N, 110 .32W', '55.453 N, 110.32W').apply(parse_lat_lon))

hmp = pd.read_table('//home/luispedro/work/metadata_collection/HMP/2017-09-06-hmp-samples.csv', index_col=0, comment='#')
hmp_lat_lon = hmp[['ena_ers_sample_id', 'latitude', 'longitude']].set_index('ena_ers_sample_id')
extra = extra.append(hmp_lat_lon.loc[[s for s in hmp_lat_lon.index if s in unexplained]])

matched = {}
unexplained -= set(extra.index)
for s in unexplained:
    sel = hmp[hmp.aliases.map(lambda a: s in a)]
    sel = sel[['latitude', 'longitude']]
    if len(sel):
        matched[s] = sel.iloc[0]
extra = extra.append(pd.DataFrame(matched).T)

extra.reset_index(inplace=True)
extra.drop_duplicates(inplace=True)
extra.set_index('index', inplace=True)

extra.to_csv('extra-location.tsv', sep='\t')
final = pd.concat([extra, compl[['Long', 'Lat']].rename(columns={'Long':'longitude', 'Lat':'latitude'})])

final.reindex(index=biome.index).dropna().to_csv('gps-coords.tsv', sep='\t')

