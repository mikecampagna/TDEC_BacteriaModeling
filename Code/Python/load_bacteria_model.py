from bacteria_model import WatershedTreatmentModel
import json
#from common.util.config_utils import get_settting

"""
_jsonIn = {
        "bmp_geometry": {"type": "Polygon", "coordinates": [[[1,2],[2,3],[1,2]]]},
        "bmp_type": "Forest Buffer",
        "bmp_id": 10,
        "nhd_comid": 4655442,
        "huc12": "000000000000",
        "dnrec_basin_id": 12,
        "drainage_ac": 10.0,
        "percent_impervious": 85.0,
        "runoff_capture_in": 1.0,
        "acres_converted": 5.0,
        "landuse_converted": "Urban",
        "converted_to": "Natural",
        "fraction_willing": 0.6,
        "awareness": 0.5,
        "n_systems_treated": 10,
        "n_systems_retired": 10,
        "sewer_miles": 20000,
        "slipline_miles": 200
}

"nhd_comid": 4655442,
"nhd_comid": 4651170,
"nhd_comid": 4652076,
"nhd_comid": 4655356,
"huc12": "020402050403",
"huc12": "020402050402",
"dnrec_basin_id": 41,
"""

_jsonIn = {
        "bmp_type": "Forest Buffer",
        "bmp_id": 10,
        "nhd_comid": 4655442,
        #"huc12": "020402050402",
        #"dnrec_basin_id": 41,
        "drainage_ac": 51.05,
}

# _jsonIn = {
#         "bmp_type": "RR",
#         "bmp_id": 17,
#         #"nhd_comid": 4655442,
#         #"huc12": "020402050402",
#         "dnrec_basin_id": 41,
#         "drainage_ac": 51.05,
#         "percent_impervious": 85.0,
#         "runoff_capture_in": 1.0
# }

# _jsonIn = {
#         "bmp_type": "ST",
#         "bmp_id": 18,
#         "nhd_comid": 4655442,
#         #"huc12": "020402050402",
#         #"dnrec_basin_id": 41,
#         "drainage_ac": 51.05,
#         "percent_impervious": 85.0,
#         "runoff_capture_in": 1.0
# }

# _jsonIn = {
#         "bmp_type": "Septic Denitrifcation and Pumping",
#         "bmp_id": 21,
#         "nhd_comid": 4655442,
#         #"huc12": "020402050402",
#         #"dnrec_basin_id": 41,
#         # Report either willingness and awareness or number treated, preferably the latter
#         # "fraction_willing": 0.4,
#         # "awareness": 0.2,
#         "n_systems_treated": 100,
# }

# _jsonIn = {
#         "bmp_type": "Septic Connection",
#         "bmp_id": 22,
#         "nhd_comid": 4655442,
#         #"huc12": "020402050402",
#         #"dnrec_basin_id": 41,
#         "n_systems_retired": 100,
# }

# _jsonIn = {
#         "bmp_type": "Pet Waste Education",
#         "bmp_id": 25,
#         "nhd_comid": 4655442,
#         #"huc12": "020402050402",
#         #"dnrec_basin_id": 41,
#         # IF NONE IS PROVIDED, willingness is 0.6
#         "fraction_willing": 0.6,
#         "awareness": 0.5
# }

# _jsonIn = {
#         "bmp_type": "Sliplines",
#         "bmp_id": 26,
#         "nhd_comid": 4655442,
#         #"huc12": "020402050402",
#         #"dnrec_basin_id": 41,
#         # USER MUST PROVIDE THE NUMBER OF SEWER MILES, WE COULD NOT GET THIS INFORMATION
#         "sewer_miles": 8500,
#         "slipline_miles": 100
# }

# _jsonIn = {
#         "bmp_type": "Land Retirement",
#         "bmp_id": 12,
#         "nhd_comid": 4655442,
#         #"huc12": "020402050402",
#         #"dnrec_basin_id": 41,
#         # USER MUST PROVIDE THE NUMBER OF SEWER MILES, WE COULD NOT GET THIS INFORMATION
#         "acres_converted": 5.0,
#         # Please use ["Urban", "Agricultural", "Impervious", "Turf"]
#         "landuse_converted": "Urban",
# }

# _jsonIn = {
#         "bmp_type": "Impervious Surface Elimination to Pervious Surface",
#         "bmp_id": 24,
#         "nhd_comid": 4655442,
#         #"huc12": "020402050402",
#         #"dnrec_basin_id": 41,
#         # USER MUST PROVIDE THE NUMBER OF SEWER MILES, WE COULD NOT GET THIS INFORMATION
#         "acres_converted": 15.0,
#         "percent_impervious": 85.0,
# }

parentpath = r'C:\Users\mcamp\Documents\GitHub\TDEC_BacteriaModeling\Code\Python\\'

config_file = json.load(open( parentpath + 'config.json' ))
PG_CONFIG = config_file['PGtest']

bmp = WatershedTreatmentModel(_jsonIn, PG_CONFIG)
bmp.execute()
outputJson = bmp.dump()

print( outputJson )
