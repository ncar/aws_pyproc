import warnings
warnings.filterwarnings('ignore', '.*the sets module is deprecated.*',DeprecationWarning, 'MySQLdb')

import MySQLdb
from xml.dom.minidom import parseString
import sys
import re
from datetime import datetime, date, timedelta
import time
import calendar
import binascii
import math
import os
import logging
import emailerror
import quality
import calculations


def get_station_id(full_path):
	logging.debug("func get_station_id")
	#get only the alphabetical parts of the filename from the full_path
	file_path = full_path.split('/')
	file_name = file_path[-1]
	fn = ""
	
	for i in file_name:
		if re.match("^[A-Za-z_]$", i):
			fn += i
		else:
			break
	
	#get aws_id based on file name
	sql = "SELECT aws_id,home_dir FROM tbl_stations WHERE filename = '" +  fn + "';"

	try:
		conn = MySQLdb.connect (host = "localhost",user = "aws",passwd = "ascetall",db = "aws")
	except MySQLdb.Error, e:
		print "Error %d: %s" % (e.args[0], e.args[1])
		logging.error("failed to connect to DB in standalone.get_station_id()\n" + str(e))
		sys.exit (1)

	cursor = conn.cursor()
	cursor.execute(sql)
	
	ret_arr = []
	
	while (1):
		row = cursor.fetchone()
		if row == None:
			break
		ret_arr.append(str(row[0]))#aws_id
		ret_arr.append(str(row[1]))#name
	
	cursor.close()
	conn.commit()
	conn.close()
	
	return ret_arr
		
def make_data_array(full_path):
	logging.debug("func make_data_array")
	#read the file
	data = []
	f = open(full_path, "rb")
	try:
		byte = f.read(1)
		while byte != "":
			data.append(int(binascii.b2a_hex(byte), 16))
			byte = f.read(1)		
	finally:
		f.close()
	
	return data

#information for this parsing supplied by Gerard Rouse of MEA (gerard@mea.com.au)
def parse_stamps(data):
	logging.debug("func parse_stamps")
	stamps = []
	#read seconds per scan
	scan_rate = (data[5] + 256*data[6])/128
	#read scans since 1980-01-01 00:00:00
	elapsed_scans = data[25] + 256*data[26] + (math.pow(256,2)*data[27]) + (math.pow(256,3)*data[28])
	#calc seconds since 1980-01-01 00:00:00
	elapsed_seconds = elapsed_scans * scan_rate
	basis = datetime(1980,1,1,00,00,00)
	#basis time + elapsed_seconds = now
	last_log_stamp = basis + timedelta(0,elapsed_seconds) # days, seconds, then other fields.
	
	#read number_of_scans * scan_rate
	log_interval = (data[29] + 256*data[30] + (math.pow(256,2)*data[31]) + (math.pow(256,3)*data[32])) * scan_rate
	log_size = data[33] + 256*data[34]
	#logging.debug("log_size = " + str(log_size))
	bytes_logged = int(data[17] + 256*data[18] + (math.pow(256,2)*data[19]) + (math.pow(256,3)*data[20]))
	num_logs = int(bytes_logged / log_size)
	
	for i in range(num_logs):
		stamps.append(last_log_stamp - + timedelta(0,i*log_interval))
	
	stamps = sorted(stamps)
	
	#for i in stamps:
	#	print i.strftime("%Y-%m-%d %H:%M:%S")

	#sort to get earliest stamp first
	return stamps

def parse_bytes_logged(data):
	logging.debug("func parse_bytes_logged")
	return int(data[17] + 256*data[18] + (math.pow(256,2)*data[19]) + (math.pow(256,3)*data[20]))

#inserts a single AWS reading into the DB
#@param:	reading		array (<col_name>,<value>)
#@param:	aws_id		string
def insert_reading(readings, aws_id):
	logging.debug("func insert_reading")
	logging.debug(str(readings))
	#make SQL
	sql = "INSERT INTO tbl_15min "
	
	logging.debug("before for col_head in readings")
	#get column headers
	sql += "(aws_id,stamp,"
	batt_dedupe = 0
	for col_head in readings[0][1]:
		#ignore heading for LMW07 pan battery
		if aws_id == 'LMW07':
			batt_dedupe = batt_dedupe + 1
			if batt_dedupe == 10 or batt_dedupe == 13:
				pass
			else:
				sql += col_head[0] + ","
		else:
			if str(col_head[0]) == 'nothing':
				pass
			else:
				sql += col_head[0] + ","
	sql = sql[:-1]#remove last ','
	sql += ")\n"
	
	logging.debug("after for col_head in readings")
	logging.debug(sql)
	
	sql += "VALUES \n"
	
	logging.debug("before for reading in readings:")
	#get the variable values
	for reading in readings:
		batt_dedupe = 0
		sql += "("
		sql += "'" + aws_id + "',"
		sql += "'" + reading[0].strftime("%Y-%m-%d %H:%M:%S") + "',"
		for column in reading[1]:
			#ignore reading for LMW07 pan battery
			if aws_id == 'LMW07':
				batt_dedupe = batt_dedupe + 1
				if batt_dedupe == 10:
					pass
				elif batt_dedupe == 13:#INSERT pan into its own table
					sql_pan = "INSERT INTO tbl_15min_pan (aws_id,stamp,pan) VALUES ('" + aws_id + "','" + reading[0].strftime("%Y-%m-%d %H:%M:%S") + "'," + str(column[1]) + ");"

					try:
						conn = MySQLdb.connect (host = "localhost",user = "aws",passwd = "ascetall",db = "aws")
					except MySQLdb.Error, e:
						logging.error("failed to connect to DB in standalone.insert_reading() for pan\n" + str(e))
						return False
					
					cursor = conn.cursor()
					try:
						cursor.execute(sql_pan)
					except MySQLdb.Error, e:
						logging.error("failed to INSERT in standalone.insert_reading() for pan\n" + str(e))
					conn.commit()
					conn.close()					
				else:		
					sql += str(column[1]) + ","
			else:
				if str(column[0]) == 'nothing':
					pass
				else:
					sql += str(column[1]) + ","
		sql = sql[:-1]
		sql += "),\n"
	logging.debug("after for reading in readings:")
	
	sql = sql[:-2] + ";"#remove last ',' and '\n'

	logging.debug(sql + "\n")
	
	#run query
	try:
		conn = MySQLdb.connect (host = "localhost",user = "aws",passwd = "ascetall",db = "aws")
	except MySQLdb.Error, e:
		logging.error("failed to connect to DB in standalone.insert_reading()\n" + str(e))
		return False
	
	cursor = conn.cursor()
	try:
		cursor.execute(sql)
	except MySQLdb.Error, e:
		logging.error("failed to INSERT in standalone.insert_reading()\n" + str(e))
	conn.commit()
	conn.close()	
	
	return sql

#read a single 15min buffer
#@param:	buff			binary data for Buffer0, trimmed by read_15min_buffers
#@param:	var_col_names	3D array (<func_name>,<db_name>,<no._bytes>),
def read_15min_buffer(buff, var_col_names):
	logging.debug("func read_15min_buffer")
	data_parsed = []
	airT = -99
	rh = -1
	Wavg = -1
	
	var_cursor = 0
	for var in var_col_names:
		#print "[" + str(var[0]) + "," + str(var[1]) + "]"
		datum = []
		datum.append(var[1])#save the column name (db_name)
		if (var[1] == "soilT"):
			if (var[0] == "parse_airT"):
				datum.append(calculations.parse_airT(float(buff[var_cursor]),float(buff[var_cursor+1])))
			elif (var[0] == "parse_soilT10"):
				datum.append(calculations.parse_soilT10(float(buff[var_cursor]),float(buff[var_cursor+1])))
		elif (var[1] == "airT" or var[1] == "canT"):#airT
			this_airT = calculations.parse_airT(float(buff[var_cursor]),float(buff[var_cursor+1]))
			if (var[1] == "airT"):
				airT = this_airT
			datum.append(this_airT)
		elif (var[1] == "rh" or var[1] == "canRH"):#rh
			this_rh = calculations.parse_rh(float(buff[var_cursor]),float(buff[var_cursor+1]))
			if (var[1] == "rh"):
				rh = this_rh
			datum.append(this_rh)
		elif (var[1] == "gsr"):#gsr
			datum.append(calculations.parse_gsr(float(buff[var_cursor]),float(buff[var_cursor+1])))
		elif (var[1] == "leaf"):#leaf
			datum.append(calculations.parse_leaf(float(buff[var_cursor])))
		elif (var[1] == "Wdir"):#Wdir[1]
			if (var[0] == "parse_Wdir"):
				datum.append(calculations.parse_Wdir(float(buff[var_cursor-1]),float(buff[var_cursor])))
			elif (var[0] == "parse_Wdir2"):
				datum.append(calculations.parse_Wdir2(float(buff[var_cursor-1]),float(buff[var_cursor]),float(buff[var_cursor+1]),float(buff[var_cursor+2])))
		elif (var[1] == "Wmin" or var[1] == "Wavg" or var[1] == "Wmax"):#Wmin/Wavg/Wmax, all use same function
			Wxxx = calculations.parse_Wavg(float(buff[var_cursor]),float(buff[var_cursor+1]))
			if (var[1] == "Wavg"):
				Wavg = Wxxx
			datum.append(Wxxx)
		elif (var[1] == "rain"):#rain, RG rain
			if (var[0] == "parse_rain"):#aws
				datum.append(calculations.parse_rain(float(buff[var_cursor]),float(buff[var_cursor+1])))
			elif (var[0] == "parse_rain_rg"):#rg
				datum.append(calculations.parse_rain_rg(float(buff[var_cursor]),float(buff[var_cursor+1])))			
		elif (var[1] == "batt"):#batt_2
			if (var[0] == "parse_batt_1"):#batt_1
				datum.append(calculations.parse_batt_1(float(buff[var_cursor])))
			elif (var[0] == "parse_batt_2"):#batt_2
				datum.append(calculations.parse_batt_2(float(buff[var_cursor]),float(buff[var_cursor+1])))
			elif (var[0] == "parse_batt_3"):#batt_3
				datum.append(calculations.parse_batt_3(float(buff[var_cursor]),float(buff[var_cursor+1])))				
			elif (var[0] == "parse_batt_rg"):#batt_rg
				datum.append(calculations.parse_batt_rg(float(buff[var_cursor]),float(buff[var_cursor+1])))
		elif (var[1] == "pressure"):
			if (var[0] == "parse_pressure"):
				datum.append(calculations.parse_pressure(float(buff[var_cursor]),float(buff[var_cursor+1])))
			elif (var[0] == "parse_pressure2"):
				datum.append(calculations.parse_pressure2(float(buff[var_cursor]),float(buff[var_cursor+1])))			
		elif (var[1] == "pan"):
			datum.append(calculations.parse_airT(float(buff[var_cursor]),float(buff[var_cursor+1])))		
		elif (var[1] == "nothing"):
			datum.append(calculations.parse_nothing(float(buff[var_cursor]),float(buff[var_cursor+1])))
			#do nothing
			pass		
		#progress the var_cursor by the number of bytes in tbl_var_translate for this variable
		var_cursor = var_cursor + int(var[2])
		data_parsed.append(datum)
	
	#calculate appT, dp, deltaT
	if airT != -99 and rh != -1 and Wavg != -1:#we have airT, rh & Wavg all calculated
		datum = []
		datum.append("appT")
		datum.append(calculations.calc_appT(rh,airT,Wavg))
		data_parsed.append(datum)
		datum = []
		datum.append("dp")
		datum.append(calculations.calc_dp(rh,airT))
		data_parsed.append(datum)
		datum = []
		datum.append("deltaT")
		datum.append(calculations.calc_deltaT(rh,airT))
		data_parsed.append(datum)				

	return data_parsed

#read all the 15min buffers
#@param stamps:			timestamps read from the data file by pares_stamps
#@param	data:			binary data 
#@param	buff_length:	int
#@param	num_logs:		int
#@param	scm_type:		enum ('file', 'db')
#@return	none (fires read_15min_buffer)
#@param:	aws_id		string
def read_15min_buffers(stamps, data, buff_length, num_logs, scm_type, aws_id, scm_file):
	logging.debug("func read_15min_buffers")
	start = 45
	var_col_names = []
	
	if scm_type == "-file":
		var_col_names = var_translate(var_order_from_string(var_order_from_SCM_file(scm_file)))
	elif scm_type == "-db":
		var_col_names = var_translate(var_order_from_string(var_order_from_DB(aws_id)))
	
	readings = []
	for i in range(num_logs):
		reading = []
		buff_start = start + i * (buff_length / num_logs)
		buff_end =  start + (i + 1) * (buff_length / num_logs)
		reading.append(stamps[i])
		reading.append(read_15min_buffer(data[buff_start:buff_end],var_col_names))
		readings.append(reading)
	
	#write the readings to the DB
	return insert_reading(readings, aws_id)

#translate MEA instrument channel names and action values into parsing functions, DB columns, bytes needed
#translation values from db: tbl_var_translate
#@param		var_order	2D array (<MEA_channel_name>,<MEA_action>),
#@return				3D array (<func_name>,<db_name>,<no._bytes>),
def var_translate(var_order):
	logging.debug("func var_translate")
	print str(var_order)
	#get the DB values in the order of var_order
	var_string = ""
	for var in var_order:
		var_string += "'" + var[0] + "_" + var[1] + "',"
	
	#trim trailing comma
	if var_string.endswith(","): 
		var_string = var_string[:-1]
	
	sql = 	'''
			SELECT func_name, db_name, bytes 
			FROM tbl_var_translate
			WHERE mea_key IN (''' + var_string + ''')
			ORDER BY FIELD(mea_key, ''' + var_string + ''');
			'''
			
	#print sql
	
	try:
		conn = MySQLdb.connect (host = "localhost",user = "aws",passwd = "ascetall",db = "aws")
	except MySQLdb.Error, e:
		logging.error("failed to connect to DB in standalone.var_translate()\n" + str(e))
		sys.exit (1)	

	cursor = conn.cursor()
	try:
		cursor.execute(sql)
	except MySQLdb.Error, e:
		logging.error("failed to SELECT in standalone.var_translate()\n" + str(e))
		sys.exit (1)	
	
	
	#3D array of func_name, db_name, bytes from tbl_var_translate
	var_col_names = []
	for row in cursor.fetchall():
		var_new = []
		for col in row:
			var_new.append(col)
		var_col_names.append(var_new)

	cursor.close()
	conn.commit()
	conn.close()
	
	#for row in var_col_names:
	#	print row
		
	return var_col_names

#get the SCM contents from the db (tbl_stations: scm)
#@param aws_id	string
#@return 		xml string
def var_order_from_DB(aws_id):
	logging.debug("func var_order_from_DB")
	#get aws_id based on file name
	sql = "SELECT scm FROM tbl_stations WHERE aws_id = '" +  aws_id + "';"

	try:
		conn = MySQLdb.connect (host = "localhost",user = "aws",passwd = "ascetall",db = "aws")
	except MySQLdb.Error, e:
		logging.error("failed to connect to DB in standalone.var_order_from_DB()\n" + str(e))
		sys.exit (1)

	cursor = conn.cursor()
	try:
		cursor.execute(sql)
	except MySQLdb.Error, e:
		logging.error("failed to SELECT in standalone.var_order_from_DB()\n" + str(e))
		sys.exit (1)
	
	ret = ""
	while (1):
		row = cursor.fetchone()
		if row == None:
			break
		ret = row[0]
	
	cursor.close()
	conn.commit()
	conn.close()
	
	return ret
	
#get the SCM contents from a file
#@return 		xml string
def var_order_from_SCM_file(full_path):
	logging.debug("func var_order_from_SCM_file")
	fh = open(full_path)
	contents = fh.read()
	fh.close()

	return contents

#read the order in which to process variable data in the Buffer0 from an SCM file
#@return 				2D array (<MEA_channel_name>,<MEA_action>),
#called by var_translate
def var_order_from_string(var_order_string):
	logging.debug("func var_order_from_string")
	dom = parseString(var_order_string)
	variables_nums = []
	#get instument order (number and action) in Buffer0 (15min)
	for node in dom.getElementsByTagName("Buffer0"):
		for node2 in node.childNodes:
			if node2.nodeName == "Entries":#toxml().replace('<Inst>','').replace('</Inst>','')
				for node3 in node2.childNodes:
					if node3.nodeName == "Entry":
						var_new = []
						for node4 in node3.childNodes:
							if node4.nodeName == "Inst":
								var_new.append(int(node4.toxml().replace('<Inst>','').replace('</Inst>','')))
							elif node4.nodeName == "Action":
								var_new.append(str(node4.toxml().replace('<Action>','').replace('</Action>','')))
									
						variables_nums.append(var_new)

	#get instruments except those with type of CALC
	variables_names = dict()
	for node in dom.getElementsByTagName("Instruments"):
		for node2 in node.childNodes:
			#regex match the number in <INST3 
			if re.match("^<INST", node2.toxml()) or re.match("^<Inst", node2.toxml()):
				#print node2.nodeName
				for node3 in node2.childNodes:
					if node3.nodeName == "Channel":
						if node2.getAttribute("Disabled") != "True":
							if node3.getAttribute("Type") != "CALC":
								num = node2.toxml().replace('<INST','').split(" ")[0]
								num = num.replace('<Inst','').split(" ")[0]
								val = node3.getAttribute("Name")
								if val == "h5" and node2.getAttribute("Name") == "PTB100_Pro":
									val = "h52"
								variables_names[int(num)] = val
							else:
								if node3.getAttribute("Name") == "WndDirxxx":
									num = node2.toxml().replace('<INST','').split(" ")[0]
									num = num.replace('<Inst','').split(" ")[0]
									val = node3.getAttribute("Name")
									variables_names[int(num)] = val									
									
		
	#reconsile buffer0 listing (nums) with instuments (names)
	variables_results = []
	for inst in variables_nums:
		if variables_names.has_key(inst[0]):
			var_res = []
			var_res.append(variables_names[inst[0]])	
			#print variables_names[inst[0]]
			var_res.append(inst[1])
			variables_results.append(var_res)
	
	#print variables_results
	
	return variables_results

#@param:	data_file	file	the data file to process (ending DMP)
#@param:	switch		string 	'-db' or '-file'
#@param:	scm_file	string	path to an SCM file if param2 is '-file'
#@return:	sql used for inserting. For debugging only
def process_file(data_file,switch="-db",scm_file=None):
	logging.debug("func process_file")
	aws_id = get_station_id(data_file)[0]
	data = make_data_array(data_file)
	stamps = parse_stamps(data)
	sql = read_15min_buffers(stamps, data, parse_bytes_logged(data), len(stamps), switch, aws_id,scm_file)
	
	return sql

#@param:	file_path	string	string path to a DMP file
#@return:	an array of hex data arrays, one for each DUMP reading in the input file
def split_multifile(file_path):
	logging.debug("func split_multifile")
	#read the file, byte by byte, find the index of the 'D's in DUMP
	D = 0
	U = 0
	M = 0
	P = 0
	cnt = 0
	data_buffer = []
	data_buffers = []
	f = open(file_path, "rb")
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
				data_buffer.pop()#M
				data_buffer.pop()#U
				data_buffer.pop()#D
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

#@param:	data_file	file	the data file to process (ending DMP)
#@param:	switch		string 	'-db' or '-file'
#@param:	scm_file	string	path to an SCM file if param2 is '-file'
def process_multi_file(data_file,switch="-db",scm_file=None):
	logging.debug("func process_multi_file")
	aws_id = get_station_id(data_file)[0]
	buffs = split_multifile(data_file)
	
	for buff in buffs:
		logging.debug("buff length: " + str(len(buff)))
		stamps = parse_stamps(buff)
		sql = read_15min_buffers(stamps, buff, parse_bytes_logged(buff), len(stamps), switch, aws_id,scm_file)			
	
#TEST
#logging.basicConfig(filename='standalone-errors.log',format='%(asctime)s %(levelname)s\n%(message)s\n\n', level=logging.DEBUG)
#split_multifile(sys.argv[1])
#print var_order_from_string(var_order_from_DB(sys.argv[1]))
