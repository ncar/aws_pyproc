import warnings
warnings.filterwarnings('ignore', '.*the sets module is deprecated.*',DeprecationWarning, 'MySQLdb')

import MySQLdb
import sys
import re
from xml.dom.minidom import parseString
import pprint
#from elementtree import ElementTree
#import elementtree.ElementTree as ET
#import cElementTree as ET
#import lxml.etree as ET
import xml.etree.ElementTree as ET
import binascii
from cStringIO import StringIO

def minutes_buffer_length(dmp_path):
    #read the file
    data = []
    f = open(dmp_path, "rb")
    try:
	byte = f.read(1)
	while byte != "":
	    data.append(int(binascii.b2a_hex(byte), 16))
	    byte = f.read(1)		
    finally:
	f.close()
    
    return data[33]

def read_SCM_file(path):
    fh = open(path)
    contents = fh.read()
    fh.close()

    return contents

def instrument_no_bytes(name):
    #how do I know this?
    if name == 'Leaf':
        no_bytes = 1
    else:
        no_bytes = 2
        
    return no_bytes

def instrument_get_data(byte_array, formula, a, b):
    no_bytes == len(byte_array)
    
    if formula == "":
        pass
    '''
    if no_bytes == 1:
    else:#2
    ''' 

def get_instruments(inputxml):
    dom = parseString(inputxml)
    instrument_nums = []
    #get instument order (number and action) in Buffer0
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
                                            
                        instrument_nums.append(var_new)

    '''
    <INST22 Model="TB-3A" Name="Rain" Description="Rain Gauge (0.2 mm)" Units="mm">
        <Channel Name="RN" Virt="Y" Type="UB"/>
        <Scaling Type="&apos;A&apos; gain &apos;B&apos;" a="0" b="0.1"/>
        <Display Type="real" Field="5.1"/>
        <Source>
            <Consts>
                <Const Name="InputChan" Value="c0" Comment="Rain gauge input channel"/>
                <Const Name="Tip" Value="0.2" Comment="Rain gauge calibration"/>
            </Consts>
        </Source>
        <Limits/>
    </INST22>    
    '''
    
    #get instruments except those with type of CALC or if they are disabled
    instruments = []
    for node in dom.getElementsByTagName("Instruments"):
        for node_INST in node.childNodes:
            #regex match the number in <INST3
            #this is an instrument
            if re.match("^<INST", node_INST.toxml()) or re.match("^<Inst", node2.toxml()):
                #only add if not disabled or if != CALC
                num = node_INST.toxml().replace('<INST','').split(" ")[0]
                num = num.replace('<Inst','').split(" ")[0]                
                instruments.append(int(num))
                for node_INST_children in node_INST.childNodes:
                    if node_INST_children.nodeName == "Channel":
                        if node_INST.getAttribute("Disabled") != "True":
                            if node_INST_children.getAttribute("Type") != "CALC":
                                num = node_INST.toxml().replace('<INST','').split(" ")[0]
                                num = num.replace('<Inst','').split(" ")[0]
                                instruments[int(num)] = {'name': node_INST.nodeName, 'model': node_INST.getAttribute("Model")}
                    if node_INST_children.nodeName == "Scaling":
                        #print "Scaling Type = " + node_INST_children.getAttribute("Type")
                        pass

    #reconcile buffer0 nums with instuments names and other values
    variables_results = []

    for inst in instrument_nums:
        if instruments.has_key(inst[0]):
            var_res = []
            var_res.append(instruments[inst[0]])	
            #print instruments[inst[0]]
            var_res.append(inst[1])
            variables_results.append(var_res)
	
    #print variables_results
    return variables_results

#pp = pprint.PrettyPrinter(depth=2)
#pp.pprint(get_instruments(read_SCM_file(sys.argv[1])))

#for instrument in instruments:
#tree = etree.parse(read_SCM_file(sys.argv[1]))
#r = tree.xpath('Buffer0')
#print r[0].tag

def get_instrument_info(path):
    tree = ET.parse(path)
    
    #get minutes instruments
    buf0 = tree.findall("//Buffer0/Entries/Entry")
    
    bufs = []
    for i in buf0:
        buf = []
        buf.append(int(list(i)[0].text))
        buf.append(list(i)[1].text)
        bufs.append(buf)
    
    #get instrument descriptions
    instruments = []
    iis = tree.findall("//Instruments/*")
    for INST in iis:
        #if list(ii)[0].get('Type') != 'CALC':
            if INST.tag[0:4] == 'INST' and INST.get('Disabled') != 'True':
                instrument = []
                instrument.append(int(INST.tag[4:]))#remove INST
                instrument.append(INST.get('Name'))
                for INST_child in INST:
                    if INST_child.tag == 'Scaling':
                        instrument.append(INST_child.get('Type'))
                        if INST_child.get('a') == None:
                            instrument.append(0)
                        else:
                            instrument.append(float(INST_child.get('a')))
                        if INST_child.get('b') == None:
                            instrument.append(0)
                        else:
                            instrument.append(float(INST_child.get('b')))
                if INST.get('Name') == 'sin' or INST.get('Name') == 'cos' or INST.get('Name') == 'LeafWet':
                    instrument.append(1)
                else:
                    instrument.append(2)
                        
                instruments.append(instrument)
    
    instrument_infos = []
    for buf in bufs:
        for instrument in instruments:
            if buf[0] == instrument[0]:
                #if instrument[2] != 'Formula':
                    instrument_info = []
                    instrument_info.append(instrument[1])	#name
                    instrument_info.append(buf[1])		#MIN, AVE, MAX, TOT2
                    instrument_info.append(instrument[2])	#forumla
                    instrument_info.append(instrument[3])	#A
                    instrument_info.append(instrument[4])	#B
                    instrument_info.append(instrument[5])	#bytes
                    
                    instrument_infos.append(instrument_info)
                    
    return instrument_infos

#get the SCM contents from the db (tbl_stations: scm)
#@param aws_id	string
#@return 		xml string
def get_scm_file_from_DB(aws_id):
	#TODO: logging.debug("func var_order_from_DB")
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

#get the SCM file from DB
f = open('x.scm','w')
f.write(get_scm_file_from_DB(sys.argv[1]))
f.close()


print 'buffer length ' + str(minutes_buffer_length(sys.argv[2]))

#process through Buffer0 up to LogLength size
cnt = 45 #just to give indecies from the start of the first buffer0, rather than zero
for reading in get_instrument_info('x.scm'):
    if cnt < (45 + minutes_buffer_length(sys.argv[2])):
        print str(reading[0]) + " " + str(reading[1]) + " " + str(reading[2]) + " " + str(reading[3]) + " " + str(reading[4]) + " " + str(reading[5])
        cnt += reading[5] #add the numer of bytes for this reading