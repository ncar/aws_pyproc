# Example: daemonize pyinotify's notifier.
# from https://github.com/seb-m/pyinotify/blob/master/python2/examples/daemon.py
# Requires Python >= 2.5
import functools
import sys
import pyinotify
import datetime
import time
import os
import shutil
import re
import traceback
import processor
import logging
import ftplib

class MyEventHandler(pyinotify.ProcessEvent):
    def my_init(self, file_object=sys.stdout):
        self._file_object = file_object
        logging.info("Turned On") 

    #react when a file is created
    def process_IN_CLOSE_WRITE(self, event):
        try:
            new_file_path = event.pathname
            #ignore directory creation and files without .DMP at end
            if os.path.isdir(new_file_path) == False and new_file_path[-3:] == "DMP":
      
                #check for zero-length files
                if os.stat(new_file_path).st_size == 0:
                  logging.info("Unprocessed, data file size is 0: " + new_file_path)
                else:
                    #string {filename}.DMP
                    file_name = new_file_path.split('/')[-1]
                    #get the aws_id of the station, from the DB, based on the file name
                    aws_info = []
                    aws_info = processor.get_station_id(new_file_path)
                    #don't process unknown station's data
                    if len(aws_info) <= 0:
                        logging.info("Unprocessed, station not known: " + new_file_path)
                    else:
                        aws_id = aws_info[0]
                        aws_dir = aws_info[1]
              
                        #read data from the file that arrived
                        fin = open(new_file_path, "rb")
                        data = fin.read()
                        fin.close()
                        
                        #check if archiving folder for this file exists
                        if not os.path.exists(aws_dir):
                            os.makedirs(aws_dir)	
                        
                        #process
                        sql = processor.process_multi_file(new_file_path)						
                  
                        if aws_id == 'ADLD01' or aws_id == 'ADLD02':
                            self.ftp_file_to_adld(aws_id, new_file_path)
                  
                        #copy/append processed file to station's home dir then delete the file that arrived
                        fout = open(aws_dir + file_name, "ab")
                        fout.write(data)
                        fout.close()
                        os.remove(new_file_path)
                  
                        logging.info("Processed: " + new_file_path)
            
            else:#ignore files not ending in DMP and size with size == 0
                  logging.info("Ignored, not DMP: " + new_file_path)     
        except IndexError, e:
            logging.error("ftpwatcherd IN_CREATE (index error) " + new_file_path + "\nError report: " + str(e))
            print traceback.format_exc()
        except Exception, e:
            logging.error("ftpwatcherd IN_CREATE " + new_file_path + "\nError report: " + str(e))
            emailerror.send("ftpwatcherd IN_CREATE " + new_file_path + "\nError report: " + str(e))
        
        return

    #
    #	FTP for Charleston & Virginia to Ad MtLR
    #
    def ftp_file_to_adld(self, aws_id, path):
        try: 
            ftp = ftplib.FTP()
            ftp.connect('links.waterdata.com.au')
            ftp.login('SAMDB','$samdb@')
            if aws_id == 'ADLD01':
                ftp.cwd('/SAMDB-AWS/Virginia/')
            if aws_id == 'ADLD02':
                ftp.cwd('/SAMDB-AWS/Chrlston/')
            fin = open(path, "rb")
            ftp.storbinary ('APPE ' + path.split('/')[-1], fin, 1)
            fin.close()
            ftp.quit()
        except Exception, e:
            logging.error("ftpwatcherd links.waterdata.com.au FTP: " + str(e))

#
#	MAIN
#
logging.basicConfig(filename='ftpw2.log',format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

try:
    wm = pyinotify.WatchManager()
    notifier = pyinotify.Notifier(wm, MyEventHandler())
    # Exclude patterns from list: i.e. the old folder
    excl = pyinotify.ExcludeFilter(['^/home/ftp/aws/old/'])
    wm.add_watch(sys.argv[1], pyinotify.IN_CLOSE_WRITE, rec=True, exclude_filter=excl)
    notifier.loop(daemonize=True, pid_file='/tmp/ftpw2.pid', stdout='/home/ftp/watcher/ftpw2.txt')
except pyinotify.NotifierError, err:
    logging.error(err)
