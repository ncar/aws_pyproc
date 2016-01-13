import logging

#
#   program settings
#
HOME_DIR = '/opt/processor/'
LOG_FILE = HOME_DIR + 'procdeamon.log'
LOG_LEVEL = logging.INFO
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
DB_PASSWD = "MEYERevap1"
DB = "aws"
