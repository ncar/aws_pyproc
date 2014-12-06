import MySQLdb
from lxml import etree
from StringIO import StringIO
import sys
import re
from datetime import datetime, timedelta
import binascii
import math
import logging
import calculations


def connect_to_aws_db(host="localhost", user="aws", passwd="ascetall", db="aws"):
    try:
        conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db)
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        logging.error("failed to connect to DB in get_station_aws_id()\n" + str(e))
        sys.exit(1)

    return conn


def get_station_details(conn, dmp_file_path):
    """
    Gets the Station aws_id & scm file from the database

    :param conn: MySQL connection object
    :param dmp_file_path: path to a DMP file
    :return: an aws_id and an SCM (XML) file as a string
    """
    logging.debug("def get_station_aws_id(" + dmp_file_path + ")")
    # get only the alphabetical parts of the filename from the full_path
    file_name = dmp_file_path.split('/')[-1]
    m = re.search("(^[A-Za-z_]*)", file_name)
    aws_name = m.group(0)

    sql = "SELECT aws_id, scm FROM tbl_stations WHERE filename = '" + aws_name + "';"

    cursor = conn.cursor()
    cursor.execute(sql)

    result = ''
    while 1:
        row = cursor.fetchone()
        if row is None:
            break
        #aws_id, scm
        result = [str(row[0]), str(row[1])]

    cursor.close()
    conn.commit()
    conn.close()

    return result


def reading_vars_from_scm(scm_doc):
    """
    Gets an array of byte conversion information for the 15min buffer of a DMP file from an SCM file

    :param scm_doc: the station's SCM file
    :return: an array of dicts
    """
    logging.debug("def var_order_from_string()")

    t = etree.parse(StringIO(scm_doc))

    #Get instrument order from Buffer0
    instance_entries = []
    for instrument_buffer in t.xpath('//Buffer0/Entries/Entry'):
        instance_entry = dict()
        for child in instrument_buffer.getchildren():
            if child.tag == 'Inst':
                instance_entry['inst'] = child.text
            if child.tag == 'Action':
                instance_entry['action'] = child.text
        instance_entries.append(instance_entry)

    #Get instrument details from Instruments
    instruments = []
    for instrument in t.xpath('/Scheme/Instruments/*'):
        if instrument.tag.startswith('INST'):
            i = dict()
            #print etree.tostring(instrument, pretty_print=True)
            i['inst'] = instrument.tag.replace('INST', '')

            for inst in instrument.getchildren():
                if inst.tag == 'Channel':
                    i['name'] = inst.get('Name')
                if inst.tag.startswith('Scal'):
                    if inst.get('Type').startswith("'A'"):
                        i['type'] = 'conversion'
                        i['conv'] = inst.get('Type')
                        i['a'] = inst.get('a')
                        i['b'] = inst.get('b')
                    elif inst.get('Type').startswith("Formula"):
                        i['type'] = 'formula'
                        i['conv'] = inst.get('Formula')
                    #print etree.tostring(inst, pretty_print=True)
            instruments.append(i)

    #Merge Buffer0 listing  with instument details
    instance_entries_with_details = []
    for instance_entry in instance_entries:
        for instrument in instruments:
            if instrument['inst'] == instance_entry.get('inst'):
                instance_entries_with_details.append(dict(instance_entry.items() + instrument.items()))

    return instance_entries_with_details


def readings_vars_additions_from_db(conn, reading_vars):
    try:
        cursor = conn.cursor()
        reading_var_additions = []
        for reading_var in reading_vars:
            sql = '''SELECT db_col_name, bytes
                     FROM tbl_var_lookup
                     WHERE mea_name = "''' + reading_var.get('name') + '''"
                     AND mea_action = "''' + reading_var.get('action') + '''";'''
            cursor.execute(sql)
            row = cursor.fetchone()
            if row is None:
                break
            reading_var_addition = reading_var
            reading_var_addition['db_col'] = str(row[0])
            reading_var_addition['bytes'] = int(row[1])
            reading_var_additions.append(reading_var_addition)
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        logging.error("failed to connect to DB in get_station_aws_id()\n" + str(e))
        sys.exit(1)

    cursor.close()
    conn.commit()
    conn.close()

    return reading_var_additions


def get_dmp_file_dumps(dmp_file_path):
    """
    Reads in a DMP file and returns the 15min readings within it as byte arrays

    :param dmp_file:
    :return: an array of readings
    """
    logging.debug("def get_dmp_file_readings(" + dmp_file_path + ")")
    #read the file, byte by byte, find the index of the 'D's in DUMP
    D = 0
    U = 0
    M = 0
    P = 0
    cnt = 0
    data_buffer = []
    data_buffers = []
    f = open(dmp_file_path, "rb")
    try:
        byte = f.read(1)
        cnt2 = 0
        while byte != "":
            if byte == 'D' and cnt > 0:
                D = cnt
            elif byte == 'U' and D == cnt - 1:
                U = cnt
            elif byte == 'M' and U == cnt - 1 and D == cnt - 2:
                M = cnt
            elif byte == 'P' and M == cnt - 1 and U == cnt - 2 and D == cnt - 3:
                #remove DUMP from last array
                data_buffer.pop()  #M
                data_buffer.pop()  #U
                data_buffer.pop()  #D
                #P not yet added

                #add the buffer to the buffers
                data_buffers.append(data_buffer)

                #reset the buffer
                data_buffer = []

                #add DUM to next array
                data_buffer.append(binascii.b2a_hex('D'))
                data_buffer.append(binascii.b2a_hex('U'))
                data_buffer.append(binascii.b2a_hex('M'))
                cnt2 = cnt2 + 1
            #print "buffer count:" + str(cnt2)
            #print "data_buffers count:" + str(len(data_buffers))

            data_buffer.append(int(binascii.b2a_hex(byte), 16))
            byte = f.read(1)
            cnt += 1

        #append the last buffer
        data_buffers.append(data_buffer)
    finally:
        f.close()

    #remove first, blank, buffer
    data_buffers.pop(0)

    #for a in data_buffers:
    #	print str(len(a)) + ", ",

    return data_buffers


def parse_bytes_logged(binary_dump):
    """
    Extracts the number of bytes logged from the DMP file entry

    :param binary_dump:
    :return: integer
    """
    logging.debug("def parse_bytes_logged")

    return int(binary_dump[17] +
               256 * binary_dump[18] +
               (math.pow(256, 2) * binary_dump[19]) +
               (math.pow(256, 3) * binary_dump[20]))


def parse_dump_timestamps(binary_dump):
    """
    Returns the timestamps, usually 4, for the readings in a DMP file DUMP entry
    Information for this parsing supplied by Gerard Rouse of MEA (gerard@mea.com.au)

    :param binary_dump:
    :return: list of timestamps
    """
    logging.debug("def parse_timestamp()")

    #read seconds per scan
    scan_rate = (binary_dump[5] + 256 * binary_dump[6]) / 128

    #read scans since 1980-01-01 00:00:00
    elapsed_scans = binary_dump[25] + \
                    256 * binary_dump[26] + \
                    (math.pow(256, 2) * binary_dump[27]) + \
                    (math.pow(256, 3) * binary_dump[28])

    #calc seconds since 1980-01-01 00:00:00
    basis = datetime(1980, 1, 1, 00, 00, 00)
    elapsed_seconds = elapsed_scans * scan_rate

    #basis time + elapsed_seconds = now
    last_log_stamp = basis + timedelta(0, elapsed_seconds)  # days, seconds, then other fields.

    #read number_of_scans * scan_rate
    log_interval = (binary_dump[29] + 256 * binary_dump[30] +
                    (math.pow(256, 2) * binary_dump[31]) +
                    (math.pow(256, 3) * binary_dump[32])) * scan_rate

    log_size = binary_dump[33] + 256 * binary_dump[34]
    bytes_logged = int(binary_dump[17] +
                       256 * binary_dump[18] +
                       (math.pow(256, 2) * binary_dump[19]) +
                       (math.pow(256, 3) * binary_dump[20]))
    num_logs = int(bytes_logged / log_size)

    timestamps = []
    for i in range(num_logs):
        timestamp = last_log_stamp - + timedelta(0, i * log_interval)
        timestamps.append(timestamp.strftime('%y-%m-%d %H:%M:%S'))

    timestamps = sorted(timestamps)

    return timestamps


def parse_dump(binary_dump, reading_vars):
    """
    Reads a 15min binary digit reading from a DMP file and returns a list of decimal readings as k/v pairs

    :param binary_reading: a 15min array from data from a DMP file
    :return: dictionary of decimal values
    """
    timestamps = parse_dump_timestamps(binary_dump)
    buff_length = parse_bytes_logged(binary_dump)
    number_of_readings = len(timestamps)
    #sql = read_15min_buffers(timestamps, binary_dump, bytes_logged, number_of_readings)

    #magic reading start number
    start = 45
    #iterate through the buffers of data within the reading
    readings = []
    for i in range(number_of_readings):
        buff_start = start + i * (buff_length / number_of_readings)
        buff_end = start + (i + 1) * (buff_length / number_of_readings)
        reading = parse_dump_reading(binary_dump[buff_start:buff_end], reading_vars)
        readings.append({'timestamp': timestamps[i], 'variables': reading})

    return {'readings': readings}


def parse_dump_reading(binary_dump_reading, reading_vars):
    sin = 0
    cos = 0
    rh = 0
    air_t = 0
    wind_avg = 0

    #region read variables
    read_vars = []
    reading_pointer = 0
    for var in reading_vars:
        #get bytes
        if var.get('name') in ['sin', 'cos', 'LeafWet']:
            bytes = 1
        elif var.get('name') in ['WndDir']:
            bytes = 0
        else:
            bytes = 2

        #get the raw number
        var_bytes = binary_dump_reading[reading_pointer:reading_pointer + bytes]
        if len(var_bytes) == 1:
            raw_number = var_bytes[0]
        elif len(var_bytes) == 2:
            #cater for negative values
            if var_bytes[1] > 100:
                if var_bytes[1] == 255:
                    datum2 = -1
                elif var_bytes[1] == 254:
                    datum2 = -2
                elif var_bytes[1] == 253:
                    datum2 = -3
            else:
                datum2 = var_bytes[1]

            raw_number = var_bytes[0] + 256 * datum2

        #get the scaled number
        if var.get('type') == 'conversion':
            if var.get('conv') == "'A' gain 'B'":
                scaled_number = round(float(var.get('b')) * raw_number + float(var.get('a')), 1)
                if var.get('name') == 'WndSpd':
                    #convert to km/h
                    scaled_number = scaled_number * 3.6
            if var.get('conv') == "'A' to 'B'":
                scale_range = (float(var.get('b')) - float(var.get('a'))) * 100 + 1
                #TODO: check range mapping. Why divide by 25600? Should it be 25500? Or 102000 for the more accurate sort?
                scaled_number = round((scale_range / 25600.0) * raw_number - 1.0, 4)

            #store vars needed for formula
            if var.get('name') == 'sin':
                sin = scaled_number
            elif var.get('name') == 'cos':
                cos = scaled_number
            elif var.get('db_col') == 'rh':
                rh = scaled_number
            elif var.get('db_col') == 'airT':
                air_t = scaled_number
            elif var.get('db_col') == 'Wavg':
                wind_avg = scaled_number

            read_vars.append({
                'value': scaled_number,
                'db_column': var.get('db_col'),
            })

        #move pointer for next value
        reading_pointer = reading_pointer + bytes
    #endregion

    #region calculated variables
    calc_vars = []
    import calculations

    wind_dir = calculations.calc_wind_dir(sin, cos)
    calc_vars.append({
        'value': wind_dir,
        'db_column': 'Wdir',
    })
    app_t = calculations.calc_app_t(rh, air_t, wind_avg)
    calc_vars.append({
        'value': app_t,
        'db_column': 'appT',
    })
    dp = calculations.calc_dp(rh, air_t)
    calc_vars.append({
        'value': dp,
        'db_column': 'dp',
    })
    wet_t = calculations.calc_wet_t(air_t, rh, dp)
    calc_vars.append({
        'value': wet_t,
        'db_column': 'wetT',
    })
    delta_t = calculations.calc_delta_t(air_t, wet_t)
    calc_vars.append({
        'value': delta_t,
        'db_column': 'deltaT',
    })
    vp = calculations.calc_vp(dp)
    calc_vars.append({
        'value': vp,
        'db_column': 'vp',
    })
    #endregion

    return read_vars + calc_vars


def generate_insert_sql(aws_id, readings):
    columns = ''
    values = ''

    pass_cnt = 0
    for reading in readings['readings']:
        #print reading['timestamp']
        values += '"' + aws_id + '","' + reading['timestamp'] + '",'
        for var in reading['variables']:
            if var['db_column'] != 'nothing':
                if pass_cnt == 0:
                    columns += var['db_column'] + ','
                values += str(var['value']) + ','
        values = values.strip(',') + '),('
        pass_cnt += 1
    columns = 'aws_id,stamp,' + columns
    values += ')'

    sql = 'INSERT INTO tbl_15min_new (' + columns.strip(',') + ') VALUES (' + values.strip(',()') + ');'

    return sql


def insert_reading(insert_statement):
    try:
        conn = MySQLdb.connect(host="localhost", user="aws", passwd="ascetall", db="aws")
    except MySQLdb.Error, e:
        logging.error("failed to connect to DB in standalone.insert_reading()\n" + str(e))
        return [False, "failed to connect to DB in standalone.insert_reading()\n" + str(e)]

    cursor = conn.cursor()
    try:
        cursor.execute(insert_statement)
        conn.commit()
        conn.close()
    except MySQLdb.Error, e:
        logging.error("failed to INSERT in standalone.insert_reading()\n" + str(e))
        return [False, "failed to INSERT in standalone.insert_reading()\n" + str(e)]

    return [True]




