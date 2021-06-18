import json
import requests
import geojson
import psycopg2
from psycopg2 import sql
from psycopg2 import extensions as ext

from shapely.geometry import LineString
from shapely.ops import transform
from shapely.geometry import shape
from functools import partial
import pyproj
from math import radians, cos, sin, asin, sqrt

# Unless explicitly stated otherwise, all areas are in acres
# Create CLass BMP
class WatershedTreatmentModel:
    def __init__(self,_jsonIn, _PG_datafiles):
        self.__bmp_type = ''                # TODO: user input parse
        self.__bmp_id = 0                   # TODO: user input parse
        self.__huc12 = '000000000000'       # TODO: user input parse
        self.__nhd_comid = 0                # TODO: user input parse

        # LandUse-LandCover Data
        self.__lulc = {}                    # dict {lc1: 0.0, lc2...}
        self.__lulc_groups = {}             # dict {lc1: {imperv: 0.0, turf: 0.0, forest: 0.0}, lc2...}
        self.__lulc_fc_emc = {}             # dict {lc1: 0.0, lc2...}
        self.__lulc_fc_export_coef = {}     # dict {lc1: 0.0, lc2...}
        self.__fraction_imperv = 0.0
        self.__fration_turf = 0.0
        self.__fration_forest = 0.0
        self.__urban_imperv_area = 0.0
        self.__total_imperv_area = 0.0
        self.__urban_turf_area = 0.0
        self.__total_turf_area = 0.0

        # Watershed Data
        self.__annual_rainfall_in = 0.0
        self.__watershed_area = 0.0
        self.__number_du = 0.0
        self.__people_per_du = 2.7
        self.__population = 0.0
        self.__wateruse_gpcd = 70
        self.__soil_fraction = {a: 13.8, b: 54.1, c: 25.4, d: 6.8}
        self.__runoff_coeffs = {a: {imperv: 0.95, turf: 0.15, forest: 0.02, rural: 0.02, active_constr: 0.5, water: 1.0},
                                b: {imperv: 0.95, turf: 0.20, forest: 0.03, rural: 0.03, active_constr: 0.5, water: 1.0},
                                c: {imperv: 0.95, turf: 0.22, forest: 0.04, rural: 0.04, active_constr: 0.5, water: 1.0},
                                d: {imperv: 0.95, turf: 0.25, forest: 0.05, rural: 0.05, active_constr: 0.5, water: 1.0}}

        # BMPs
        self.__bmp_discounts = {capture: 0.9, design: 1.2, maintenance: 0.9}



        self.__GeomBmp  = []
        self.__GeomWatershed = []
        self.__Description =  ''
        self.__PG_datafiles = {}
        self.__PG_Connection = ()
        self.__AcresTreated = 0.0
        self.__Implemented = False
        self.__NumberOfUnits = 0.0

        ## Run input functions to load JSON objects
        self.setSettings(_jsonIn, _PG_datafiles)

    # Load input json
    # Load coefs from settings
    def setSettings(self,_jsonIn, _PG_datafiles):
        for _key in _jsonIn.keys():
            if _key == 'bmp_geometry':
                self.setGeomBmp(_jsonIn[_key])

            if _key == 'bmp_type':
                self.setBmpType(_jsonIn[_key])

            if _key == 'bmp_id':
                self.setBmpType(_jsonIn[_key])

            if _key == 'nhd_comid':
                self.setBmpType(_jsonIn[_key])

            if _key == 'huc12':
                self.setBmpType(_jsonIn[_key])

            if _key == 'drainage_ac':
                self.setAcresTreated(_jsonIn[_key])

            if _key == 'percent_impervious':
                self.setPercentImpervious(_jsonIn[_key])

            if _key == 'runoff_capture_in':
                self.setRunoffCapture(_jsonIn[_key])

            if _key == 'acres_converted':
                self.setRunoffCapture(_jsonIn[_key])

            if _key == 'landuse_converted':
                self.setRunoffCapture(_jsonIn[_key])

            if _key == 'converted_to':
                self.setRunoffCapture(_jsonIn[_key])

            if _key == 'fraction_willing':
                self.setRunoffCapture(_jsonIn[_key])

            if _key == 'awareness':
                self.setRunoffCapture(_jsonIn[_key])

            if _key == 'n_systems_treated':
                self.setRunoffCapture(_jsonIn[_key])

            if _key == 'n_systems_retired':
                self.setRunoffCapture(_jsonIn[_key])

            if _key == 'sewer_miles':
                self.setRunoffCapture(_jsonIn[_key])

            if _key == 'slipline_miles':
                self.setRunoffCapture(_jsonIn[_key])


        self.__PG_Connection = psycopg2.connect(
                host=_PG_datafiles["host"] ,
                database=_PG_datafiles["database"] ,
                user=_PG_datafiles["user"] ,
                password=_PG_datafiles["password"] ,
                port= _PG_datafiles["port"] )

        with open('config/lookupSettings.json') as _jf:
            settings = json.load(_jf)
            self.__Coefs = settings['coefs']
            self.__Load_Coefs_Kgacreyr = settings['load_coefs_kgacreyr']
            self.__Pollutants = settings['pollutants']
            self.__BmpTypes = set(settings['bmptypes'])
            self.__Animals = settings['animals']
            self.__Delivery = settings['delivery']
            #if self.__Watershed == 'Delaware':
            #    self.__Lulcs = settings['lulc']['nlcd2011']
            #elif self.__Watershed == 'Delaware':
            #    #self.__Lulcs = settings['lulc']['cb2009']

        __PG_datafiles = _PG_datafiles

    def __str__(self):
        _return = ''
        if self.isValid():
            pass
        else:
            print("Reductions not yet computed")
        return _return

    def __len__(self):
        if self.isValid():
            return 1
        else:
            return 0

    def setBmpType(self, _value):
        self.__BmpType = _value

    def getBmpType(self):
        return self.__BmpType

    def getAcresTreated(self):
        # If they did not provide the acres treated, assume that the polygon submitted is the drainage area
        if self.__AcresTreated == 0.0:
            if self.getBmpGroup() == 'Urban Stormwater Management':
                shapein = shape(self.getGeomWatershed())
                proj = partial(pyproj.transform, pyproj.Proj(init='epsg:4326'), pyproj.Proj(init='epsg:32618'))
                shapein_proj = transform(proj, shapein)
                self.setAcresTreated(shapein_proj.area / 4046.86)
                print('no acres given, use: {}'.format(shapein_proj.area / 4046.86))
            elif eval(str(self.getGeomBmp()))['type'] == 'Polygon':
                shapein = shape(self.getGeomBmp())
                proj = partial(pyproj.transform, pyproj.Proj(init='epsg:4326'), pyproj.Proj(init='epsg:32618'))
                shapein_proj = transform(proj, shapein)
                self.setAcresTreated(shapein_proj.area/4046.86)
                print('no acres given, use: {}'.format(shapein_proj.area/4046.86))
        return self.__AcresTreated

    def getPercentImpervious(self):
        if self.__PercentImpervious == 0.0:
            impervious = 0.0
            if '21' in self.__Lulcs:
                impervious += self.__Lulcs['22'] * 0.10
            if '22' in self.__Lulcs:
                impervious += self.__Lulcs['22'] * 0.35
            if '23' in self.__Lulcs:
                impervious += self.__Lulcs['23'] * 0.65
            if '24' in self.__Lulcs:
                impervious += self.__Lulcs['24'] * 0.90
            self.setPercentImpervious(impervious)
            print('no impervious acres given, use: {}\nEstimated from LULC in watershed.'.format(impervious))
        return self.__PercentImpervious

    def getRunoffCapture(self):
        if self.__RunoffCapture == 0.0:
            self.setRunoffCapture(1.0)
            print('no runoff capture given, use: {}'.format(1.0))
        return self.__RunoffCapture

    def haversine(self, lat1,lon1,lat2,lon2):
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        """
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        r = 20922430  # Radius of earth in ft
        return c * r

    #TODO: Make sure that this length calculation works across both watersheds
    def setBmpLength(self):
        coords1 = list(geojson.utils.coords(geojson.loads(str(self.getGeomBmp()).replace("\'", "\""))))
        coords2 = [t[::-1] for t in coords1]
        distance = 0.0
        for i in range(0, len(coords2)-1):
            point1 = coords2[i]
            point2 = coords2[i + 1]
            distance += self.haversine(point1[0], point1[1], point2[0], point2[1])
        print(distance)
        self.__BmpLength = distance

    def setHuc12(self):
        _geom = json.dumps(eval(str(self.getGeomBmp())))
        try:
            # TODO: Need to set the DB up with the data and some functions
            self.__PG_Connection.set_isolation_level(0)
            _cur = self.__PG_Connection.cursor()
            if eval(str(self.getGeomBmp()))['type'] == 'Point':
                _cur.execute("SELECT * FROM databmpapi.get_huc12_pt(%s);", (_geom,))
            elif eval(str(self.getGeomBmp()))['type'] == 'LineString':
                _cur.execute("SELECT * FROM databmpapi.get_huc12_ln(%s);", (_geom,))
                self.setBmpLength()
            else:
                _cur.execute("SELECT * FROM databmpapi.get_huc12(%s);", (_geom,))
                if self.getBmpLength() == 0:
                    self.setBmpLength()
            _data = _cur.fetchall()
            self.__PG_Connection.commit()
        except Exception as _e:
            print(_e)
            data = {"NOT IN BASIN": 0}

        try:
            self.__Huc12 = ''.join(_data[0])
        except Exception as e:
            print("Could not connect to the database.\n" + str(e))

    # To do mike call get information from postgres server # might want to combine with the setHuc12. above
    def setLoad_Coefs_Kgacreyr(self):
        if self.__Huc12[:4] in self.__drbHuc04:
            if self.getBmpGroup() == 'Urban Stormwater Management':
                try:
                    self.__PG_Connection.set_isolation_level(0)
                    _cur = self.__PG_Connection.cursor()
                    _q1 = "SELECT * FROM databmpapi.get_loadedload_drb(\'{}\');".format(self.getHuc12())
                    _cur.execute(_q1)
                    _load = _cur.fetchall()
                    self.__PG_Connection.commit()
                except Exception as _e:
                    print("Could not connect to the database to retrive loading:\n" + str(_e))
            else:
                try:
                    self.__PG_Connection.set_isolation_level(0)
                    _cur = self.__PG_Connection.cursor()
                    _q1 = "SELECT * FROM databmpapi.get_load_drb(\'{}\');".format(self.getHuc12())
                    _cur.execute(_q1)
                    _load = _cur.fetchall()
                    self.__PG_Connection.commit()
                except Exception as _e:
                    print("Could not connect to the database to retrive loading:\n" + str(_e))
        elif any(self.__Huc12[:4] in s for s in self.__cbbHuc04):
            if self.getBmpGroup() == 'Urban Stormwater Management':
                try:
                    self.__PG_Connection.set_isolation_level(0)
                    _cur = self.__PG_Connection.cursor()
                    _q1 = "SELECT * FROM databmpapi.get_loadedload_cbb(\'{}\');".format(self.getHuc12())
                    _cur.execute(_q1)
                    _load = _cur.fetchall()
                    self.__PG_Connection.commit()
                except Exception as _e:
                    print("Could not connect to the database to retrive loading:\n" + str(_e))
            else:
                try:
                    self.__PG_Connection.set_isolation_level(0)
                    _cur = self.__PG_Connection.cursor()
                    _q1 = "SELECT * FROM databmpapi.get_load_cbb(\'{}\');".format(self.getHuc12())
                    _cur.execute(_q1)
                    _load = _cur.fetchall()
                    self.__PG_Connection.commit()
                except Exception as _e:
                    print("Could not connect to the database to retrive loading:\n" + str(_e))
        else:
            print("Cannot Calculate LULC: Invalid geom for BMP.")
        try:
            self.__Load_Coefs_Kgacreyr = json.loads(_load[0][0])
        except Exception as e:
            print("Could not connect to the database.\n"+str(e))

    # Get watershed boundary if input geometry is okay.
    def setGeomWatershed(self):
        if self.getGeomBmp():
            _url = 'http://watersheds.cci.drexel.edu/api/watershedboundary/'
            _payload = json.dumps(eval(str(self.getGeomBmp())))
            _headers = {}
            _r = requests.post(_url, data =_payload, headers= _headers)
            if _r.reason == 'Internal Server Error':
                _GeomWatershed = 'NULL'
                print("Cannot Calculate Watershed: Invalid geometry for BMP.")
            else:
                _GeomWatershed = _r.text
            self.__GeomWatershed = _GeomWatershed
        else:
            print("Cannot Calculate Watershed: Invalid geometry for BMP.")

    # Get LULC distribution for watershed, call DRB v CBB API
    def setLulc(self):
        if self.__Huc12[:4] in self.__drbHuc04:
            _url = "http://watersheds.cci.drexel.edu/api/fzs/"
            _payload = geojson.loads(json.dumps(self.__GeomWatershed))
            _headers = {}
            _r = requests.post(_url, data=_payload, headers=_headers)
            _Lulcs = eval(_r.text)
            for k, v in _Lulcs.items():
                _Lulcs[k] = (v)/4046.86
            # TODO: Once the Fast Zonal API is fixed for bigger NLCD Grid We shouldn't need this
            try:
                if _Lulcs['12'] > 0:
                    _Lulcs = {k: _Lulcs[k] - _Lulcs.get('12', 0) for k in _Lulcs}
                    #_Lulcs = {k:v for k,v in _Lulcs.items() if v != 0}
            except Exception as e:
                pass
            self.__Lulcs = _Lulcs
        elif any(self.__Huc12[:4] in s for s in self.__cbbHuc04):
            _url = "http://watersheds.cci.drexel.edu/api/fzs_cb10m/"
            _payload = geojson.loads(json.dumps(self.__GeomWatershed))
            _headers = {}
            _r = requests.post(_url, data=_payload, headers=_headers)
            _Lulcs = eval(_r.text)
            for k, v in _Lulcs.items():
                _Lulcs[k] = (v)/4046.86
            self.__Lulcs = _Lulcs
        else:
            print("Cannot Calculate LULC: Invalid geom for BMP.")

    # TODO: Update this function to work with the specific DB output
    def reductionRiparian(self):
        if not self.isValid():
            for _pollutant in self.__Pollutants:
                if self.__Huc12[:4] in self.__drbHuc04:
                    try:
                        for _lulc in self.__Load_Coefs_Kgacreyr[_pollutant]:
                            #print("THIS IS POLUTANT {} THIS IS LULC {}".format(_pollutant,_lulc))
                            #print("THIS IS THE LULC VALUE {}".format(self.__Lulcs[_lulc]))
                            _value = self.__Coefs[self.getBmpType()][_pollutant] \
                                 * self.__Load_Coefs_Kgacreyr[_pollutant][str(_lulc)] \
                                 * self.__Lulcs[_lulc]
                            self.__Polutants_Reduction_Kgyr[_pollutant] += _value
                    except Exception as e:
                        pass
                    try:
                        for _lulc in self.getLulcs():
                            # print("THIS IS POLUTANT {} THIS IS LULC {}".format(_pollutant,_lulc))
                            # print("THIS IS THE LULC VALUE {}".format(self.__Lulcs[_lulc]))
                            _value = self.__Coefs[self.getBmpType()][_pollutant] \
                                     * self.__Load_Coefs_Kgacreyr[_pollutant][str(_lulc)] \
                                     * self.__Lulcs[_lulc]
                            self.__Polutants_Reduction_Kgyr[_pollutant] += _value
                    except Exception as e:
                        print("Couldn't calculate load reduction in the DRB.\nContact: msc94@drexel.edu")
                elif any(self.__Huc12[:4] in s for s in self.__cbbHuc04):
                    for _lulc in self.getLulcs():
                        #print("THIS IS POLUTANT {} THIS IS LULC {}".format(_pollutant,_lulc))
                        #print("THIS IS THE LULC VALUE {}".format(self.__Lulcs[_lulc]))
                        _value = self.__Coefs[self.getBmpType()][_pollutant] \
                             * self.__Load_Coefs_Kgacreyr[_pollutant][str(_lulc)] \
                             * self.__Lulcs[_lulc]
                        self.__Polutants_Reduction_Kgyr[_pollutant] += _value
        self.setValid(True)

    def landLoads(self, lu_rid):
        for _pollutant in self.__Pollutants:
            _value = self.__Coefs[self.getBmpType()][_pollutant] \
                     * self.__Load_Coefs_Kgacreyr[_pollutant][lu_rid] \
                     * self.getAcresTreated()
            self.__Polutants_Reduction_Kgyr[_pollutant] += _value

    def reductionAgriculturalLand(self):
        if not self.isValid():
            _pas_type = ['Grazing Land Protection', 'Prescribed Grazing']
            if self.getBmpType() in _pas_type:
                if self.__Huc12[:4] in self.__drbHuc04:
                    self.landLoads('81')
                elif any(self.__Huc12[:4] in s for s in self.__cbbHuc04):
                    self.landLoads('5')
            else:
                if self.__Huc12[:4] in self.__drbHuc04:
                    self.landLoads('82')
                elif any(self.__Huc12[:4] in s for s in self.__cbbHuc04):
                    self.landLoads('0')
        self.setValid(True)

    def reductionStreamRestoration(self):
        for _pollutant in self.__Pollutants:
            if eval(str(self.getGeomBmp()))['type'] == 'LineString':
                self.__Polutants_Reduction_Kgyr[_pollutant] += self.__Coefs[self.getBmpType()][_pollutant] * self.getBmpLength()
            elif self.getBmpType() == 'Watering Facility':
                print('WATERING FACILITY HEYY')
                self.__Polutants_Reduction_Kgyr[_pollutant] += self.__Coefs[self.getBmpType()][_pollutant] * 209.0
            elif eval(str(self.getGeomBmp()))['type'] == 'Polygon':
                self.__Polutants_Reduction_Kgyr[_pollutant] += self.__Coefs[self.getBmpType()][_pollutant] * self.getBmpLength()
            else:
                self.__Polutants_Reduction_Kgyr[_pollutant] += self.__Coefs[self.getBmpType()][_pollutant] * self.getStreamFeetTreated()
        for _pollutant in self.__Pollutants:
            if self.getNumberOfUnits() > 1:
                self.__Polutants_Reduction_Kgyr[_pollutant] = self.__Polutants_Reduction_Kgyr[_pollutant] * self.getNumberOfUnits()



    # NOTE: this one does not actually need Coefs
    def landLoadsReduce(self, lu_rid_pre, lu_rid_post):
        for _pollutant in self.__Pollutants:
            _value = self.__Load_Coefs_Kgacreyr[_pollutant][lu_rid_pre] \
                     * self.getAcresTreated()
            _value -= self.__Load_Coefs_Kgacreyr[_pollutant][lu_rid_post] \
                     * self.getAcresTreated()
            self.__Polutants_Reduction_Kgyr[_pollutant] += _value

    def landLoadsReduceOneAcre(self, lu_rid_pre, lu_rid_post):
        for _pollutant in self.__Pollutants:
            _value = self.__Load_Coefs_Kgacreyr[_pollutant][lu_rid_pre] \
                     * 1.0 * self.getNumberOfUnits()
            _value -= self.__Load_Coefs_Kgacreyr[_pollutant][lu_rid_post] \
                     * 1.0 * self.getNumberOfUnits()
            self.__Polutants_Reduction_Kgyr[_pollutant] += _value

    def reductionLandUseChange(self):
        if not self.isValid():
            _fore_type = ['Tree and Shrub Establishment', 'Tree Planting']
            _area_type = ['Conservation Easement']
            if self.getBmpType() in _fore_type:
                if self.__Huc12[:4] in self.__drbHuc04:
                    self.landLoadsReduce('82','41')
                elif any(self.__Huc12[:4] in s for s in self.__cbbHuc04):
                    self.landLoadsReduce('0', '1')
            elif self.getBmpType() in _area_type:
                if self.__Huc12[:4] in self.__drbHuc04:
                    self.landLoadsReduce('82', '81')
                elif any(self.__Huc12[:4] in s for s in self.__cbbHuc04):
                    self.landLoadsReduce('0', '5')
            else:
                if self.__Huc12[:4] in self.__drbHuc04:
                    self.landLoadsReduceOneAcre('82', '81')
                elif any(self.__Huc12[:4] in s for s in self.__cbbHuc04):
                    self.landLoadsReduceOneAcre('0', '5')
        self.setValid(True)

    def reductionAgricultrualAnimals(self):
        for _pollutant in self.__Pollutants:
            for _animal in self.__Animals:
                # print("Animal: {}".format(_animal))
                # print("Pollutant: {}".format((_pollutant)))
                # print(self.__Coefs[self.getBmpType()])
                # print(self.__Animals[_animal])
                # print(self.getBmpAnimalsTreated()[_animal])
                # print(self.__Delivery[_pollutant])
                try:
                    _value = self.__Coefs[self.getBmpType()][_pollutant] \
                         * self.__Animals[_animal][_pollutant] \
                         * self.getBmpAnimalsTreated()[_animal] \
                         * self.__Delivery[_pollutant]
                    self.__Polutants_Reduction_Kgyr[_pollutant] += _value
                except:
                    print("Animal {} was not reported.".format(_animal))
        pass

    def st_performance_curve_coeffs(self, capture):
        x = capture
        self.__Coefs[self.getBmpType()]['tn'] = round(0.0152*x**5 - 0.131*x**4 + 0.4581*x**3 - 0.8418*x**2 + 0.8536*x - 0.0046,2)
        self.__Coefs[self.getBmpType()]['tp'] = round(0.0239*x**5 - 0.2058*x**4 + 0.7198*x**3 - 1.3229*x**2 + 1.3414*x - 0.0072,2)
        self.__Coefs[self.getBmpType()]['tss'] = round(0.0304*x**5 - 0.2619*x**4 + 0.916*x**3 - 1.6837*x**2 + 1.7072*x - 0.0091,2)

    def rr_performance_curve_coeffs(self, capture):
        x = capture
        self.__Coefs[self.getBmpType()]['tn'] = round(0.0308*x**5 - 0.2562*x**4 + 0.8634*x**3 - 1.5285*x**2 + 1.501*x - 0.013,2)
        self.__Coefs[self.getBmpType()]['tp'] = round(0.0304*x**5 - 0.2619*x**4 + 0.9161*x**3 - 1.6837*x**2 + 1.7072*x - 0.0091,2)
        self.__Coefs[self.getBmpType()]['tss'] = round(0.0326*x**5 - 0.2806*x**4 + 0.9816*x**3 - 1.8039*x**2 + 1.8292*x - 0.0098,2)

    def reductionUrbanGSI(self):
        for _pollutant in self.__Pollutants:
            for _lulc in self.__Load_Coefs_Kgacreyr[_pollutant]:
                try:
                    # print(self.__Lulcs)
                    # print("THIS IS POLUTANT {} THIS IS LULC {}".format(_pollutant,_lulc))
                    # print("THIS IS THE LULC VALUE {}".format(self.__Lulcs[_lulc]))
                    _value = self.__Coefs[self.getBmpType()][_pollutant] \
                             * self.__Load_Coefs_Kgacreyr[_pollutant][str(_lulc)] \
                             * self.__Lulcs[_lulc]
                    self.__Polutants_Reduction_Kgyr[_pollutant] += _value
                except:
                    print('Could not find this land cover: {}'.format(_lulc))

    # TODO: create long form description of The bmp modelling
    def setDescription(self):
        self.__Description = "The Load reductions for this {} Best Management Practice (BMP), located in Hydrologic Unit Code {}. ".format(
                                self.getBmpType()
                                ,self.getHuc12()
                                ) + \
                             "The BMP covered an area of {} acres in size with a drainage area of {}. The Land cover within the drainage area is {}.".format(
                                self.getHuc12()
                                ,self.getHuc12()
                                ,self.getHuc12())

    def kgyear_to_lbyear(self, kgyr):
        return kgyr * 2.20462

    # Func to calculate the BMP reductions
    def execute(self):
        self.setHuc12()
        self.setGeomWatershed()
        self.setLulc()
        self.setLoad_Coefs_Kgacreyr()

        if self.getBmpGroup() == 'Polygon Drainage':
            self.reductionRiparian()
        elif self.getBmpGroup() == 'Agricultural Land':
            self.reductionAgriculturalLand()
        elif self.getBmpGroup() == 'Exclusion Buffer':
            self.reductionRiparian()
            self.reductionStreamRestoration()
        elif self.getBmpGroup() == 'Stream Restoration':
            self.reductionStreamRestoration()
        elif self.getBmpGroup() == 'Land Use Change':
            self.reductionLandUseChange()
        elif self.getBmpGroup() == 'Agricultural Animal':
            self.reductionAgricultrualAnimals()
        elif self.getBmpGroup() == 'Urban Stormwater Management':
            _st_bmp_type = ["Constructed Wetland", "Dry Extended Detention Ponds", "Wet Pond", "Wet Ponds & Wetlands", "Stormwater Performance Standard-Stormwater Treatment"]
            print(self.__Coefs[self.getBmpType()])
            if self.getBmpType() in _st_bmp_type:
                self.st_performance_curve_coeffs(self.getRunoffCapture())
            else:
                self.rr_performance_curve_coeffs(self.getRunoffCapture())
            print(self.__Coefs[self.getBmpType()])
            self.reductionUrbanGSI()
        else:
            print('The BMP group was not found within the list of BMP groups.')
        self.setDescription()
        self.__PG_Connection.close()

    # Func to export bmp to JSON
    def dump(self):
        if self.isValid:
            _jsonOut = '{'
            _jsonOut += '"bmp_geometry":' + json.dumps(self.__GeomBMP) + ',' + chr(10)
            _jsonOut += '"bmp_type": ' + json.dumps(self.__BmpType) + ',' + chr(10)
            _jsonOut += '"bmp_group": ' + json.dumps(self.__BmpGroup) + ',' + chr(10)
            _jsonOut += '"watershed_geometry": ' + self.__GeomWatershed + ',' + chr(10)
            _jsonOut += '"reduction_lbyr": ' + json.dumps(self.__Polutants_Reduction_lbyr) + ',' + chr(10)
            _jsonOut += '"huc12": ' + json.dumps(self.__Huc12) +  ',' + chr(10)
            _jsonOut += '"reduction_lbyr_coeffs": ' + json.dumps(self.__Load_Coefs_lbacreyr ) + ',' + chr(10)
            _jsonOut += '"description": ' + json.dumps(self.__Description ) + chr(10)
            _jsonOut += '}'
            # print(json.loads(_jsonOut))
            return _jsonOut
        else:
            return "Not Valid cannot export JSON"
