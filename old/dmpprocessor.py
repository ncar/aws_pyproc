import warnings
warnings.filterwarnings('ignore', '.*the sets module is deprecated.*',DeprecationWarning, 'MySQLdb')

import re, sys, os
import MySQLdb
from datetime import datetime
import logging
import dmpprocessor
import lxml.etree
import glob

class DmpProcessor():
    #process a new (incoming) CSV file
    def process(self, scm_file):
        logging.info("processing " + scm_file)
        cols = self.get_buffer0_sensors(scm_file)
        self.make_sql(scm_file, cols)

    def get_plat_id(self, ext_id):
        #1. create ftp_userid for individual platform
        #2. populate platforms with the individual platform data, using ext_id = ftp_userid
        #3. this function is just a SELECT plat_id FROM platforms WHERE ext_id = 'XX';, get XX from filename/FTP folder
        pass
        
        
    def get_buffer0_sensors(self, scm_file):
        doc = lxml.etree.parse(scm_file)
        es = doc.xpath(".//Buffer0/Entries/Entry")

        cols = ""
        for e in es:
            #print e.xpath("Inst/text()")[0] + ", " + e.xpath("Action/text()")[0]
            cols = cols + "," + self.get_sensor_description(scm_file, e.xpath("Inst/text()")[0]) + "_" + e.xpath("Action/text()")[0]
        
        return cols

    def get_sensor_description(self, scm_file, sensor_num):
        doc = lxml.etree.parse(scm_file)
        s_name = doc.xpath(".//Instruments/INST" + str(sensor_num) + "/@Name")
        s_col = doc.xpath(".//Instruments/INST" + str(sensor_num) + "/Channel/@Name")
        #return s_name[0] + ", " + s_col[0]
        return s_col[0]
    
    def make_sql(self, scm_file, cols):
        ext_id = scm_file.split("/")[-1].split(".")[0]
        print ext_id
        #self.get_plat_id(ext_id)
        print "INSERT INTO plat_junior (plat_id,stamp," + cols[1:] + ") VALUES "
    
if __name__ == "__main__":
    logging.basicConfig(filename="dmpprocessor.log",level=logging.DEBUG,datefmt='%Y-%m-%d %H:%M:%S',format='%(asctime)s %(levelname)s %(message)s')
    if len(sys.argv) == 2:
        dp = dmpprocessor.DmpProcessor()
        dp.process(sys.argv[1])
        #dp.get_sensor_description(sys.argv[1],103)
    else:
        sys.exit(2)