from bacteria_model import WatershedTreatmentModel
import json
#from common.util.config_utils import get_settting


_jsonIn = {
  "bmp_name": "Forest Buffer",
  "bmp_id": 10,
  "huc12": "020402070301",
  "nhd_comid": 8074660
}

parentpath = r'C:\Users\mcamp\Documents\GitHub\TDEC_BacteriaModeling\Code\Python\\'

config_file = json.load(open( parentpath + 'config.json' ))
PG_CONFIG = config_file['PGtest']

#PG_datafiles = get_settting("PG", config_file)

bmp = BestManagementPractice(_jsonIn, PG_CONFIG)
bmp.execute()
outputJson = bmp.dump()

print( outputJson )
