import logging

#
#   program settings
#
#HOME_DIR = '/opt/procdeamon/'
HOME_DIR = 'c:/work/aws_pyproc/'
LOG_FILE = HOME_DIR + 'procdeamon.log'
LOG_LEVEL = logging.DEBUG
PID_FILE = HOME_DIR + 'procdeamon.pid'
STD_OUT_LOG = HOME_DIR + 'std_out.log'

#
#   watcher settings
#
WATCHED_DIR = '/opt/data/incoming/'
ARCHIVE_DIR = '/opt/data/archive/'
WATCHED_DIR_EXCLUSIONS = []

#
#   DB settings
#
DB_HOST = "localhost"
DB_USER = "aws"
DB_PASSWD = "ASCEshort1"
DB = "aws"