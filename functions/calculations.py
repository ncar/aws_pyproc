import math


#
#	derived data calculation functions
#
def calc_wind_dir(sin, cos):
    scaled_number = round((math.atan2(cos, sin) * 180) / math.pi, 1)
    if scaled_number < 0:
        scaled_number = scaled_number + 360
    return scaled_number


def calc_es(air_t):
    #http:#www.bom.gov.au/info/thermal_stress/#apparent
    return 6.108 * math.exp((17.27 * air_t) / (237.3 + air_t))


def calc_ew(air_t):
    return math.pow(10, (0.66077 + (7.5 * air_t / (237.3 + air_t))))


def calc_ew_rh(ew, rh):
    return ew * rh / 100


def calc_e(rh, air_t):
    #e = (rh / 100) * 6.105 * math.exp(17.27 * airT/(237.7 + airT))
    #http://www.faqs.org/faqs/meteorology/temp-dewpoint/
    return (rh / 100) * 0.6105 * math.exp(17.27 * air_t / (237.3 + air_t))


def calc_dp(rh, air_t):
    #dp = round(0.66077 - math.log(ew_rh,10) * 237.3/(math.log(ew_rh,10) - 8.16077),1)
    e = calc_e(rh, air_t)
    return round((116.9 + 237.3 * math.log(e)) / (16.78 - math.log(e)), 1)


def calc_gamma():
    return 0.00066 * 100


def calc_delta(rh, air_t, dp):
    e = calc_e(rh, air_t)
    return (4098 * e) / math.pow(dp + 237.3, 2)


def calc_wet_t(air_t, rh, dp):
    gamma = calc_gamma()
    delta = calc_delta(rh, air_t, dp)
    return round((gamma * air_t + delta * dp) / (gamma + delta), 1)


def calc_ea(dp):
    return 0.6108 * math.exp((17.27 * dp) / (dp + 237.3))


def calc_app_t(rh, air_t, wind_avg):
    #derived from Adelaide Airport
    e = calc_e(rh, air_t)
    return round(air_t + 0.33 * e - 0.70 * wind_avg * 1.85 / 7 - 4, 1)


def calc_delta_t(air_t, wet_t):
    return round(air_t - wet_t, 1)


def calc_vp(dp):
    #from http://www.bom.gov.au/climate/austmaps/about-vprp-maps.shtml
    #vapour pressure = exp (1.8096 + (17.269425 * dewpoint)/(237.3 + dewpoint))
    return round(math.exp(1.8096 + (17.269425 * dp)/(237.3 + dp)), 1)


#
#   MEA functions
#
def direction(sin, cos):
    pass


def FAOEvap(aitT_min, airT_avg):
    pass


def SteadmanAT(airT, rh, WndSpd, gsr):
    pass


def DewpointCalc(airT, rh):
    pass


twenty_eight = 28
def DeltaT_Alt(airT, rh, twenty_eight):
    pass


def sigma_theta(sin, cos):
    pass


zero = 0
three_point_six = 3.6
def calcgain(WndSpd, zero, three_point_six):
    pass


