import warnings
warnings.filterwarnings('ignore', '.*the sets module is deprecated.*',DeprecationWarning, 'MySQLdb')

import MySQLdb
import sys
import re
from datetime import datetime, date, timedelta
import time
import calendar
import binascii
import math
import emailerror
import quality
import os

#
#	data parsing functions
#
def parse_Wmin(datum1, datum2):
	return round((datum1 + 256*datum2)*0.036,1)
	
def parse_Wavg(datum1, datum2):
	return round((datum1 + 256*datum2)*0.036,1)
	
def parse_Wmax(datum1, datum2):
	return round((datum1 + 256*datum2)*0.036,1)
	
def parse_Wdir(datum1, datum2):
	s = (201.0/25600.0) * datum1 - 1.0
	c = (201.0/25600.0) * datum2 - 1
	r = math.sqrt(math.pow(s,2) + math.pow(c,2))
	s2 = math.asin(s/r)
	c2 = math.acos(c/r)
	if (s2 > 0):
		return round(c2 * (180/math.pi))
	else:
		return round(360 - c2 * (180/math.pi))

#Wdir for Joyce & Symon. Uses 4 bytes		
def parse_Wdir2(datum1, datum2, datum3, datum4):
	s = (201.0/25600.0) * datum1 - 1.0
	c = (201.0/25600.0) * datum2 - 1
	r = math.sqrt(math.pow(s,2) + math.pow(c,2))
	s2 = math.asin(s/r)
	c2 = math.acos(c/r)
	if (s2 > 0):
		return round(c2 * (180/math.pi))
	else:
		return round(360 - c2 * (180/math.pi))		

def parse_airT(datum1, datum2):
	#cater for negative values
	if (datum2 > 10):
		if (datum2 == 255):
			datum2 = -1
		elif (datum2 == 254):
			datum2 = -2
		elif (datum2 == 253):
			datum2 = -3
			
	return round(float((datum1 + 256*datum2))/10,1)	

def parse_rh(datum1, datum2):
	return round(float(datum1 + 256*datum2)/10,1)
	
def parse_soilT(datum1, datum2):
	return round(float(datum1 + 256*datum2)/10,1)
	
def parse_soilT10(datum1, datum2):
	return round(float(datum1 + 256*datum2)/100,1)
	
def parse_gsr(datum1, datum2):
	return round(float(datum1 + 256*datum2),1)
	
def parse_rain(datum1, datum2):
	return round(float(datum1 + 256*datum2)/10,1)
	
def parse_rain_rg(datum1, datum2):
	return round(float(datum1 + 256*datum2)/5,1)
	
def parse_batt_1(datum1):
	return round(float(datum1) * 0.11,1)

def parse_batt_2(datum1, datum2):
	#scale values from logger's scheme file
	return round(float(datum1 + 256*datum2)*0.001221 + 1.7,1)

def parse_batt_3(datum1, datum2):
	#scale values from logger's scheme file
	return round(float(datum1 + 256*datum2)*0.01 + 0,1)

def parse_batt_4(datum1, datum2):
	#scale values from logger's scheme file
	return round(float(datum1 + 256*datum2)*0.001705 + 1.7,1)
	
def parse_batt_5(datum1, datum2):
        #scale values from logger's scheme file
        return round(float(datum1 + 256*datum2)*0.001705 + 0,1)

def parse_batt_rg(datum1, datum2):
	return round(float(datum1 + 256*datum2)/100,1)
	
def parse_leaf(datum):
	return float(datum)

def parse_canT(datum1, datum2):
	#cater for negative values
	if (datum2 > 10):
		if (datum2 == 255):
			datum2 = -1
		elif (datum2 == 254):
			datum2 = -2
		elif (datum2 == 253):
			datum2 = -3
			
	return round(float((datum1 + 256*datum2))/10,1)
	
def parse_canRH(datum1, datum2):
	return round(float(datum1 + 256*datum2)/10)
	
def parse_pressure(datum1, datum2):
	#scale values from logger's scheme file
	#=((BK27+256*BL27)*(1/2550)*(1.221)*(1060-800)+800)
	p = datum1 + 256*datum2
	p = p / 2550 # scaling
	p = p * 1.221 #firmware statement = 20
	p = p * (1060 - 800) + 800 #mapping 
	return round(p,0)
	
def parse_pressure2(datum1, datum2):
	#scale values from logger's scheme file
	p = datum1 + 256*datum2
	p = p / 5000 # scaling
	p = p * 0.155 #firmware statement = 30
	p = p * (1060 - 800) + 800 #mapping 
	return round(p,0)

#a stub function for skipping unknown or unimplemented value processing
def parse_nothing(datum1, datum2):
	return -99
	
#
#	derived data calculation functions
#	
def calc_es(airT):
	#http:#www.bom.gov.au/info/thermal_stress/#apparent
	return 6.108 * math.exp((17.27 * airT)/(237.3 + airT))

def calc_ew(airT):
	return math.pow(10,(0.66077 + (7.5 * airT / (237.3 + airT))))
	
def calc_ew_rh(ew,rh):
	return ew * rh / 100

def calc_e(rh,airT):
	#e = (rh / 100) * 6.105 * math.exp(17.27 * airT/(237.7 + airT))
	#http://www.faqs.org/faqs/meteorology/temp-dewpoint/
	return (rh / 100) * 0.6105 * math.exp(17.27 * airT/(237.3 + airT))
	
def calc_dp(rh,airT):
	#dp = round(0.66077 - math.log(ew_rh,10) * 237.3/(math.log(ew_rh,10) - 8.16077),1)
	e = calc_e(rh,airT)
	return round((116.9 + 237.3 * math.log(e))/(16.78 - math.log(e)),1)

def calc_gamma():
	return 0.00066*100
	
def calc_delta(e,dp):
	return (4098 * e)/math.pow(dp + 237.3,2)
	
def calc_wetT(gamma,airT,delta,dp):
	#wetT = 0.567 * airT + 0.393 * e + 3.94
	return (gamma * airT + delta * dp) / (gamma + delta)

def calc_ea(dp):
	return 0.6108 * math.exp((17.27 * dp)/(dp + 237.3))

def calc_appT(rh,airT,Wavg):
	#appT = round(airT + 0.348 * ew_rh - 0.70 * Wavg + 0.7 * gsr / (Wavg + 10) - 4.25,1)
	#appT = round(airT + (0.348 * e) - (0.7 * Wavg * 1.85) + (0.7 * gsr) / (Wavg * 1.85 + 10) - 4.25,1)
	#derived from Adelaide Airport
	e = calc_e(rh,airT)
	return round(airT + 0.33 * e - 0.70 * Wavg * 1.85 /7 - 4,1)

def calc_deltaT(rh,airT):
	e = calc_e(rh,airT)
	gamma = calc_gamma()
	dp = calc_dp(rh,airT)
	delta = calc_delta(e,dp)
	wetT = calc_wetT(gamma,airT,delta,dp)
	return round(airT - wetT, 1)
