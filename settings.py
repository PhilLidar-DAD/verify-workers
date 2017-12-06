import multiprocessing

# Database settings (MariaDB/Galera)
DB_HOST = 'galera01.prd.dream.upd.edu.ph'
DB_PORT = '3307'
DB_NAME = 'verify_workers'
DB_USER = 'verify_workers'
DB_PASS = 'wS7BHFEcR5q7BSPmTr7C'

# Worker settings
CPU_USAGE = .5
WORKERS = int(multiprocessing.cpu_count() * CPU_USAGE)

# Verify settings
BINS = ['gdalinfo.exe', 'ogrinfo.exe', 'lasinfo.exe', '7za.exe', 'sha1sum.exe']
RASTERS = ['.tif', '.tiff', '.adf', '.ovr', '.asc', '.png', '.jp2', '.pix']
VECTORS = ['.shp', '.kml', '.dxf']
GEOMS = ['point', 'line', 'polygon']
LAS = ['.las', '.laz']
ARCHIVES = ['.7z', '.rar', '.001', '.zip']

# Mapped network drive settings
MAP_DRV_USER = r'AD\datamanager'
MAP_DRV_PASS = 'magazinerarelycreate'
MAP_DRV_DOMN = '.prd.dream.upd.edu.ph'

# Reports
SHEETS = {
    'DPC/ARC': '1WR4w1EnYubJQ51XBTviJqNivkBdq1NLO5JzZ714G7ag',
    'DPC/LMS': '1PPpAfetRff5vNSysgpTUT5bY0dMaIjsqT3pUhzx4IgE',
    'DPC/TERRA': '1ubeopJZRzE0PCj0VOQH5f0l7HoZzgscGzt2ZjropBrM',
    'Summary': '1P5FIado7tciKa6GAqjlvRcIZuJOrFu2FtBFWu2yBrNY',
    'DPC/TERRA/LAS_Tiles': '1gTH3nMyfzuy_37_y5ViUWQBojuA1pxjf29jhZvdFgAM',
    'ftp01': '1CO18EhX1ErboasNDk013cRfdPYU31Z4haxbQ3cs7cu8',
    'GISDATA': '1MlvqiGTaT2glxa-LwWTqkO3aFToDRZZbyrRpfBl52I8',
    'EXCHANGE': '1cAS6Q0jomaCm_Noj45jArgRGEqd2nLakWp0gyUdmXeY',
    'DAC/RAWDATA': '1NCF_Q7HGozV_SdyOvczA9Ou0lMDD6zGTYiFE2QZjRRY'
}

# Block name path token index
BLOCK_NAME_INDEX = {
    'DPC/LMS/Areas': 4,
    'DPC/LMS/DATA_FOR_ARCHIVING': 5,
    'DPC/LMS/For_Terra': 4,
    'DPC/TERRA/Adjusted_LAZ_Tiles': 5,
    'DPC/TERRA/Adjusted_Orthophotos': 5,
    'DPC/TERRA/DPC_Recover': 3,
    'DPC/TERRA/LAS_Tiles': 4,
    'DPC/TERRA/Photos': 4,
}
