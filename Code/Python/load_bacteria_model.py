from bacteria_model import WatershedTreatmentModel
import json
#from common.util.config_utils import get_settting


_jsonIn = {
  "bmp_geometry": {
    "type": "Polygon","coordinates": [ [ [-75.0289808774247, 40.030406052337355],[-75.02751102688454, 40.0312932778299],[-75.02708187344216, 40.030915386901405],[-75.0285839104905, 40.02997886557678],[-75.0289808774247, 40.030406052337355] ] ]
  },
  "bmp_type": "Bioretention",
  "bmp_group": "Urban Stormwater Management",

  "acres_treated": 0.0,
  "percent_impervious": 0.0,
  "runoff_capture": 0.0
}

parentpath = r'C:\Users\mcamp\Documents\GitHub\TDEC_BacteriaModeling\Code\Python\\'

config_file = json.load(open( parentpath + 'config.json' ))
PG_CONFIG = config_file['PGtest']

#PG_datafiles = get_settting("PG", config_file)

bmp = BestManagementPractice(_jsonIn, PG_CONFIG)
bmp.execute()
outputJson = bmp.dump()

print( outputJson )
