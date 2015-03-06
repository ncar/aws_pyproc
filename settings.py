import logging

HOME_DIR = '/opt/procdeamon/'
LOG_FILE = HOME_DIR + 'procdeamon.log'
LOG_LEVEL = logging.DEBUG
PID_FILE = HOME_DIR + 'procdeamon.pid'
STD_OUT_LOG = HOME_DIR + 'std_out.log'

WATCHED_DIR = '/home/ftp/incoming/'
ARCHIVE_DIR = '/opt/archive/'
#WATCHED_DIR_EXCLUSIONS = ['/home/ftp/incoming']