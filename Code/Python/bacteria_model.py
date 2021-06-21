import json
import requests
import psycopg2
from psycopg2 import sql
from psycopg2 import extensions as ext

# Unless explicitly stated otherwise, all areas are in acres
# Create CLass BMP
class WatershedTreatmentModel:
    def __init__(self,_jsonIn, _pg_datafiles):
        self.__bmp_geometry = None
        self.__bmp_type = None
        self.__bmp_id = None
        self.__nhd_comid = None
        self.__huc12 = None
        self.__dnrec_basin_id = None
        self.__drainage_ac = 0.0
        self.__percent_impervious = 0.0
        self.__runoff_capture_in = 1.0
        self.__acres_converted = 0.0
        self.__landuse_converted = 'None'
        self.__converted_to = 'None'
        self.__fraction_willing = 0.6
        self.__awareness = 0.0
        self.__n_systems_treated = 0.0
        self.__n_systems_retired = 0.0
        self.__sewer_miles = 0.0
        self.__slipline_miles = 0.0

        self.__bac_load_data = []
        self.__watershed_population = 0.0
        self.__watershed_du = 0.0
        self.__bac_reduction = 0.0
        self.__bac_initial_load = 0.0
        self.__bac_reduced_load = 0.0
        self.__coefs = []
        self.__bmp_names = []
        self.__watershed_urb_load = 0.0
        self.__watershed_urb_area = 0.0
        self.__wqv_watershed = 0.0
        self.__n_septic_systems = 0.0
        self.__watershed_septic_load = 0.0
        self.__watershed_septic_bn_yr = 0.0
        self.__petwaste_bnmpn_yr = 0.0
        self.__urbanloading_bnmpn_acyr = 0.0
        self.__imploading_bnmpn_acyr = 0.0
        self.__turfloading_bnmpn_acyr = 0.0
        self.__agloading_bnmpn_acyr = 0.0
        self.__natloading_bnmpn_acyr = 0.0

        self.__description = 'None'

        self.__pg_datafiles = {}
        self.__pg_connection = ()

        ## Run input functions to load JSON objects
        self.setSettings(_jsonIn, _pg_datafiles)

    # Load input json
    # Load coefs from settings
    def setSettings(self,_jsonIn, _pg_datafiles):
        for _key in _jsonIn.keys():
            if _key == 'bmp_geometry':
                self.__bmp_geometry = _jsonIn[_key]

            if _key == 'bmp_type':
                self.__bmp_type = _jsonIn[_key]

            if _key == 'bmp_id':
                self.__bmp_id = _jsonIn[_key]

            if _key == 'nhd_comid':
                self.__nhd_comid = _jsonIn[_key]

            if _key == 'huc12':
                self.__huc12 = _jsonIn[_key]

            if _key == 'dnrec_basin_id':
                self.__dnrec_basin_id = _jsonIn[_key]

            if _key == 'drainage_ac':
                self.__drainage_ac = _jsonIn[_key]

            if _key == 'percent_impervious':
                self.__percent_impervious = _jsonIn[_key] / 100.0

            if _key == 'runoff_capture_in':
                self.__runoff_capture_in = _jsonIn[_key]

            if _key == 'acres_converted':
                self.__acres_converted = _jsonIn[_key]

            if _key == 'landuse_converted':
                self.__landuse_converted = _jsonIn[_key]

            if _key == 'converted_to':
                self.__converted_to = _jsonIn[_key]

            if _key == 'fraction_willing':
                self.__fraction_willing = _jsonIn[_key]

            if _key == 'awareness':
                self.__awareness = _jsonIn[_key]

            if _key == 'n_systems_treated':
                self.__n_systems_treated = _jsonIn[_key]

            if _key == 'n_systems_retired':
                self.__n_systems_retired = _jsonIn[_key]

            if _key == 'sewer_miles':
                self.__sewer_miles = _jsonIn[_key]

            if _key == 'slipline_miles':
                self.__slipline_miles = _jsonIn[_key]


        self.__pg_connection = psycopg2.connect(
                host=_pg_datafiles["host"] ,
                database=_pg_datafiles["database"] ,
                user=_pg_datafiles["user"] ,
                password=_pg_datafiles["password"] ,
                port= _pg_datafiles["port"] )

        with open('lookupSettings.json') as _jf:
            settings = json.load(_jf)
            self.__coefs = settings['coefs']
            self.__bmp_names = settings['bmp_names']

        __pg_datafiles = _pg_datafiles

    # To do mike call get information from postgres server # might want to combine with the setHuc12. above
    def setBacteriaLoadData(self):
        if self.__nhd_comid != None:
            try:
                self.__pg_connection.set_isolation_level(0)
                _cur = self.__pg_connection.cursor()
                _q = "SELECT * FROM dnrec.nhdplus_tdec_bacterialoading WHERE comid = {};".format(self.__nhd_comid)
                _cur.execute(_q)
                _load = _cur.fetchall()
                self.__pg_connection.commit()
            except Exception as _e:
                print("Could not connect to the database to retrive loading:\n" + str(_e))
        elif self.__huc12 != None:
            try:
                self.__pg_connection.set_isolation_level(0)
                _cur = self.__pg_connection.cursor()
                _q = "SELECT * FROM dnrec.huc12_tdec_bacterialoading WHERE huc12 LIKE '{}';".format(self.__huc12)
                _cur.execute(_q)
                _load = _cur.fetchall()
                self.__pg_connection.commit()
            except Exception as _e:
                print("Could not connect to the database to retrive loading:\n" + str(_e))
        elif self.__dnrec_basin_id != None:
            try:
                self.__pg_connection.set_isolation_level(0)
                _cur = self.__pg_connection.cursor()
                _q = "SELECT * FROM dnrec.dnrecws_tdec_bacterialoading WHERE dnrecws = {};".format(self.__dnrec_basin_id)
                _cur.execute(_q)
                _load = _cur.fetchall()
                self.__pg_connection.commit()
            except Exception as _e:
                print("Could not connect to the database to retrive loading:\n" + str(_e))
        else:
            print('No valid geographic scale given.')

        try:
            self.__bac_load_data = list(_load[0])
            self.__watershed_population = float(self.__bac_load_data[1])
            self.__watershed_du = float(self.__bac_load_data[2])
            self.__watershed_urb_load = float(self.__bac_load_data[33])
            self.__watershed_imp_load = float(self.__bac_load_data[34])
            self.__watershed_turf_load = float(self.__bac_load_data[35])
            self.__watershed_ag_load = float(self.__bac_load_data[36])
            self.__watershed_nat_load = float(self.__bac_load_data[37])

            self.__watershed_urb_area = float(self.__bac_load_data[7])
            self.__watershed_imp_area = float(self.__bac_load_data[8])
            self.__watershed_turf_area = float(self.__bac_load_data[9])
            self.__watershed_ag_area = float(self.__bac_load_data[10])
            self.__watershed_nat_area = float(self.__bac_load_data[11])

            self.__wqv_watershed = float(self.__bac_load_data[13])
            self.__n_septic_systems = float(self.__bac_load_data[3])
            self.__watershed_septic_load = float(self.__bac_load_data[4])
            self.__watershed_septic_bn_yr = float(self.__bac_load_data[43])
            self.__petwaste_bnmpn_yr = float(self.__bac_load_data[5])

            self.__urbanloading_bnmpn_acyr = float(self.__bac_load_data[28])
            self.__imploading_bnmpn_acyr = float(self.__bac_load_data[29])
            self.__turfloading_bnmpn_acyr = float(self.__bac_load_data[30])
            self.__agloading_bnmpn_acyr = float(self.__bac_load_data[31])
            self.__natloading_bnmpn_acyr = float(self.__bac_load_data[32])

        except Exception as e:
            print("Could not connect to the database.\n"+str(e))

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

    # TODO: create long form description of The bmp modelling
    def setDescription(self):
        self.__description = "The Load reductions for this {} Best Management Practice (BMP), located in Hydrologic Unit Code {}. ".format(
                                self.__bmp_type
                                ,self.__huc12
                                ) + \
                             "The BMP is estimated to reduce bacteria by {} bnMPN/year.".format(self.__bac_reduction)

    def reduction_riparian(self):
        # The spreadsheet WTM uses only the watershed impervious acres to define treatability for total urban load
        # I think it is more accurate to use the impervious + turf acres?
        # _watershed_urban_load = 2352733.0
        # _watershed_imp_area = 4754.38
        self.__bac_initial_load = self.__watershed_urb_load

        _treatability = self.__drainage_ac / self.__watershed_urb_area
        print(self.__coefs[self.__bmp_type]["discount_rate"])
        _reduction = self.__watershed_urb_load \
                     * _treatability \
                     * self.__coefs[self.__bmp_type]["tot_removal_rate"] \
                     * self.__coefs[self.__bmp_type]["discount_rate"]
        self.__bac_reduction = _reduction
        self.__bac_reduced_load = self.__bac_initial_load - self.__bac_reduction

    def calc_wqv(self, capture, imperv, perv, imperv_coef, perv_coef):
        return capture * (imperv * imperv_coef + perv * perv_coef) * 3630

    def reduction_rr(self):
        self.__bac_initial_load = self.__watershed_urb_load

        _imperv_area = self.__drainage_ac * self.__percent_impervious
        _perv_area = self.__drainage_ac - _imperv_area
        _wqv_bmp = self.calc_wqv(self.__runoff_capture_in, _imperv_area, _perv_area,
                                 self.__coefs[self.__bmp_type]["imperv_runoff_coef"],
                                 self.__coefs[self.__bmp_type]["perv_runoff_coef"])
        _capture = _wqv_bmp / self.__wqv_watershed
        _reduction = self.__watershed_urb_load \
                     * _capture \
                     * self.__coefs[self.__bmp_type]["tot_removal_rate"] \
                     * self.__coefs[self.__bmp_type]["discount_rate"]
        print(_reduction)
        self.__bac_reduction = _reduction
        self.__bac_reduced_load = self.__bac_initial_load - self.__bac_reduction

    def reduction_st(self):
        self.__bac_initial_load = self.__watershed_urb_load

        _imperv_area = self.__drainage_ac * self.__percent_impervious
        _perv_area = self.__drainage_ac - _imperv_area
        _wqv_bmp = self.calc_wqv(self.__runoff_capture_in, _imperv_area, _perv_area,
                                 self.__coefs[self.__bmp_type]["imperv_runoff_coef"],
                                 self.__coefs[self.__bmp_type]["perv_runoff_coef"])
        _capture = _wqv_bmp / self.__wqv_watershed
        _reduction = self.__watershed_urb_load \
                     * _capture \
                     * self.__coefs[self.__bmp_type]["tot_removal_rate"] \
                     * self.__coefs[self.__bmp_type]["discount_rate"]
        self.__bac_reduction = _reduction
        self.__bac_reduced_load = self.__bac_initial_load - self.__bac_reduction

    def reduction_septic_den_pump(self):
        self.__bac_initial_load = self.__watershed_septic_load

        _p_near_water = self.__coefs[self.__bmp_type]["p_near_waterway"]
        _p_not_near_water = 1.0 - _p_near_water
        if self.__n_systems_treated > 0.0:
            if self.__n_systems_treated > self.__n_septic_systems:
                self.__n_systems_treated = self.__n_septic_systems
            _fraction_treated = self.__n_systems_treated / self.__n_septic_systems
        elif self.__fraction_willing > 0.0:
            _fraction_treated = self.__fraction_willing * self.__awareness
            self.__n_systems_treated = _fraction_treated * self.__n_septic_systems
        else:
            _fraction_treated = 0.0
            print('Warning, output is zero if not all parameters are supplied (need fraction treated).')
        # TODO: What is wrong with this equation from WTM? I fixed it, but don't fully understand what is happening...
        # _new_failure_rate = (self.__n_septic_systems * self.__coefs[self.__bmp_type]["base_failure_rate"]
        #                     - self.__n_systems_treated * self.__coefs[self.__bmp_type]["retired_failure_rate"]) \
        #                     * (1.0 - _fraction_treated) \
        #                     / (self.__n_septic_systems - self.__n_systems_treated)
        _new_failure_rate = (self.__n_septic_systems * self.__coefs[self.__bmp_type]["base_failure_rate"]) \
                            * (1.0 - _fraction_treated) \
                            / (self.__n_septic_systems + self.__n_systems_treated)
        _reduction = self.__bac_initial_load - \
                     (self.__watershed_septic_bn_yr * _new_failure_rate
                      * (self.__coefs[self.__bmp_type]["norm_del_ratio"] * _p_not_near_water * self.__coefs[self.__bmp_type]["norm_bac_decay"]
                         + self.__coefs[self.__bmp_type]["adj_del_ratio"] * _p_near_water * self.__coefs[self.__bmp_type]["adj_bac_decay"]))
        self.__bac_reduction = _reduction
        self.__bac_reduced_load = self.__bac_initial_load - self.__bac_reduction

    def reduction_septic_conn(self):
        self.__bac_initial_load = self.__watershed_septic_load

        _p_near_water = self.__coefs[self.__bmp_type]["p_near_waterway"]
        _p_not_near_water = 1.0 - _p_near_water
        if self.__n_systems_retired > 0.0:
            if self.__n_systems_retired > self.__n_septic_systems:
                self.__n_systems_retired = self.__n_septic_systems
            _retire_ratio = self.__n_systems_retired / self.__n_septic_systems
        _sewage_treated = self.__watershed_septic_bn_yr * _retire_ratio
        _reduction = (_sewage_treated * self.__coefs[self.__bmp_type]["base_failure_rate"]
                      * self.__coefs[self.__bmp_type]["norm_del_ratio"]
                      * self.__coefs[self.__bmp_type]["norm_bac_decay"]
                      * _p_not_near_water) + (_sewage_treated * self.__coefs[self.__bmp_type]["base_failure_rate"]
                      * self.__coefs[self.__bmp_type]["adj_del_ratio"]
                      * self.__coefs[self.__bmp_type]["adj_bac_decay"]
                      * _p_near_water)
        _extra_plant_load = _reduction * 10**(-self.__coefs[self.__bmp_type]["bac_log_redux"])
        _reduction -= _extra_plant_load
        self.__bac_reduction = _reduction
        self.__bac_reduced_load = self.__bac_initial_load - self.__bac_reduction

    def reduction_pet_waste(self):
        self.__bac_initial_load = self.__petwaste_bnmpn_yr
        _reduction = self.__bac_initial_load * self.__fraction_willing * self.__awareness
        self.__bac_reduction = _reduction
        self.__bac_reduced_load = self.__bac_initial_load - self.__bac_reduction

    def reduction_sliplines(self):
        if self.__slipline_miles > self.__sewer_miles:
            self.__slipline_miles = self.__sewer_miles
        self.__bac_initial_load = self.__sewer_miles * self.__coefs[self.__bmp_type]["overflow_per_mi"] \
                                  * self.__coefs[self.__bmp_type]["vol_per_overflow"] \
                                  * self.__coefs[self.__bmp_type]["bn_bac_constant"]
        _load_change = (self.__sewer_miles - self.__slipline_miles) * self.__coefs[self.__bmp_type]["overflow_per_mi"] \
                                  * self.__coefs[self.__bmp_type]["vol_per_overflow"] \
                                  * self.__coefs[self.__bmp_type]["bn_bac_constant"]
        _reduction = self.__bac_initial_load - _load_change
        self.__bac_reduction = _reduction
        self.__bac_reduced_load = self.__bac_initial_load - self.__bac_reduction

    def reduction_land_retirement(self):
        _loading_rate_change = 0.0
        # IF NONE IS PROVIDED, ASSUME IT IS "URBAN"
        if self.__landuse_converted == 'Urban':
            if self.__acres_converted > self.__watershed_urb_area:
                self.__acres_converted = self.__watershed_urb_area
            self.__bac_initial_load = self.__watershed_urb_load
            _loading_rate_change = self.__urbanloading_bnmpn_acyr - self.__natloading_bnmpn_acyr

        elif self.__landuse_converted == 'Agricultural':
            if self.__acres_converted > self.__watershed_ag_area:
                self.__acres_converted = self.__watershed_ag_area
            self.__bac_initial_load = self.__watershed_ag_load
            _loading_rate_change = self.__agloading_bnmpn_acyr - self.__natloading_bnmpn_acyr

        elif self.__landuse_converted == 'Impervious':
            print(self.__watershed_imp_area)
            if self.__acres_converted > self.__watershed_imp_area:
                self.__acres_converted = self.__watershed_imp_area
            self.__bac_initial_load = self.__watershed_imp_load
            _loading_rate_change = self.__imploading_bnmpn_acyr - self.__natloading_bnmpn_acyr

        elif self.__landuse_converted == 'Turf':
            if self.__acres_converted > self.__watershed_turf_area:
                self.__acres_converted = self.__watershed_turf_area
            self.__bac_initial_load = self.__watershed_nat_load
            _loading_rate_change = self.__turfloading_bnmpn_acyr - self.__natloading_bnmpn_acyr

        else:
            if self.__acres_converted > self.__watershed_urb_area:
                self.__acres_converted = self.__watershed_urb_area
            self.__bac_initial_load = self.__watershed_urb_load
            _loading_rate_change = self.__urbanloading_bnmpn_acyr - self.__natloading_bnmpn_acyr

        _reduction = self.__acres_converted * _loading_rate_change
        self.__bac_reduction = _reduction
        self.__bac_reduced_load = self.__bac_initial_load - self.__bac_reduction

    def reduction_impervious_elim(self):
        _imp_acres_converted = self.__acres_converted * self.__percent_impervious
        print(self.__watershed_imp_area)
        print(_imp_acres_converted)
        if _imp_acres_converted > self.__watershed_imp_area:
            self.__acres_converted = self.__watershed_imp_area
        self.__bac_initial_load = self.__watershed_imp_load
        _loading_rate_change = self.__imploading_bnmpn_acyr - self.__natloading_bnmpn_acyr
        _reduction = _imp_acres_converted * _loading_rate_change
        self.__bac_reduction = _reduction
        self.__bac_reduced_load = self.__bac_initial_load - self.__bac_reduction

    # Func to calculate the BMP reductions
    def execute(self):
        self.setBacteriaLoadData()

        if self.__bmp_type == 'Forest Buffer':
            self.reduction_riparian()
        elif self.__bmp_type in ['RR', 'Runoff Reduction']:
            self.reduction_rr()
        elif self.__bmp_type in ['ST', 'Stormwater Treatment']:
            self.reduction_st()
        elif self.__bmp_type in ['Septic Denitrifcation and Pumping']:
            self.reduction_septic_den_pump()
        elif self.__bmp_type in ['Septic Connection']:
            self.reduction_septic_conn()
        elif self.__bmp_type in ['Pet Waste Education']:
            self.reduction_pet_waste()
        elif self.__bmp_type in ['Sliplines', 'Sliplines (miles)']:
            self.reduction_sliplines()
        elif self.__bmp_type in ['Land Retirement']:
            self.reduction_land_retirement()
        elif self.__bmp_type in ['Impervious Surface Elimination to Pervious Surface',
                                 'Impervious surface elimination to pervious surface']:
            self.reduction_impervious_elim()
        else:
            print('The BMP group was not found within the list of BMP groups.')
        self.setDescription()
        self.__pg_connection.close()

    # Func to export bmp to JSON
    def dump(self):
        try:
            _jsonOut = '{'
            _jsonOut += '"bmp_type": ' + json.dumps(self.__bmp_type) + ',' + chr(10)
            _jsonOut += '"initial_load_bnmpn_yr": ' + json.dumps(self.__bac_initial_load) + ',' + chr(10)
            _jsonOut += '"reduced_load_bnmpn_yr": ' + json.dumps(self.__bac_reduced_load) + ',' + chr(10)
            _jsonOut += '"reduction_bnmpn_yr": ' + json.dumps(self.__bac_reduction) + ',' + chr(10)

            _jsonOut += '"acs_population": ' + json.dumps(self.__watershed_population) + ',' + chr(10)
            _jsonOut += '"acs_dwelling_units": ' + json.dumps(self.__watershed_du) + ',' + chr(10)

            _jsonOut += '"description": ' + json.dumps(self.__description ) + chr(10)
            _jsonOut += '}'
            # print(json.loads(_jsonOut))
            return _jsonOut
        except:
            return "Not Valid cannot export JSON"
