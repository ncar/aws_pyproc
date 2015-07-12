import settings
import functions
import MySQLdb
import logging
import sys
from operator import sub
import re


def get_results_from_tbls(conn, aws_id, table, date_stamp):
    sql = '''
        SELECT
            ROUND(SUM(airT),1),
            ROUND(SUM(appT),1),
            ROUND(SUM(dp),1),
            ROUND(SUM(rh),1),
            ROUND(SUM(deltaT),1),
            ROUND(SUM(soilT),1),
            ROUND(SUM(gsr),1),
            ROUND(SUM(Wmin),1),
            ROUND(SUM(Wavg),1),
            ROUND(SUM(Wmax),1),
            ROUND(SUM(rain),1)
        FROM ''' + table + '''
        WHERE aws_id = "''' + aws_id + '''"
        AND DATE(stamp) = "''' + date_stamp + '''"
        AND HOUR(stamp) < '23';
    '''
    try:
        if conn is None:
            conn = functions.connect_to_aws_db()

        cursor = conn.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        return row
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        logging.error("failed to connect to DB in get_station_details()\n" + str(e))
        sys.exit(1)


def test_one_file_processing(conn, dmp_file_path, date_stamp):
    # get station details
    [aws_id, scm] = functions.get_station_details(conn, dmp_file_path)

    # process the file
    functions.process(dmp_file_path)

    # compare the processed file's results to the test results
    a = get_results_from_tbls(conn, aws_id, 'tbl_15min_test', date_stamp)
    b = get_results_from_tbls(conn, aws_id, 'tbl_data_minutes', date_stamp)

    return results_are_same(compare_table_results(a, b))


def compare_table_results(tbl_15min_test_results, tbl_data_minutes_results):
    return tuple(map(sub, tbl_15min_test_results, tbl_data_minutes_results))


def results_are_same(compare_result):
    guestimate = 0
    for val in compare_result:
        guestimate += val

    if guestimate < 1.5:
        return True
    else:
        return False


def get_stamp(dmp_file_path):
    file_name = dmp_file_path.split('/')[-1]
    d = re.match('([A-Za-z]+)([0-9]{6})[.]*', file_name)
    d1 = d.group(2)
    d2 = '20' + d1[0] + d1[1] + '-' + d1[2] + d1[3] + '-' + d1[4] + d1[5]
    return d2


def clear_all_test_results(conn):
    try:
        if conn is None:
            conn = functions.connect_to_aws_db()

        cursor = conn.cursor()
        cursor.execute('DELETE FROM tbl_data_minutes')
        row = cursor.fetchone()
        return row
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        logging.error("failed to connect to DB in get_station_details()\n" + str(e))
        sys.exit(1)


if __name__ == "__main__":
    # marshall the files for testing
    TEST_DATA_DIR = settings.HOME_DIR + 'test/'
    TEST_FILES = [
        'ALDINGA150704-0000.DMP',  # pass 0
        'CADELL150704-0000.DMP',  # pass 1
        'LEWIS150704-0000.DMP',  # pass 2
        'LEWIS150701-0000.DMP',  # pass 3
        'LEWIS150101-0000.DMP',  # pass 4
        'NARRUNG150704-0000.DMP',  # pass 5
        'SandyBor150704-0000.DMP',  # pass 6
        'SEAVIEW150704-0000.DMP',  # pass 7
        'SELLICKS150704-0000.DMP',  # pass 8
        'Wellngtn150704-0000.DMP',  # pass 9
        'Wemen150704-0000.DMP',  # pass 10
        'SYMON150704-0000.DMP',  # pass 11
        'Watarru150704-0000.DMP',  # pass 12
        'JOYCE150101-0000.DMP',  # pass 13
        'KUIPTO140101-0000.DMP',  # pass  14
    ]
    '''
    # test all files
    date_stamp = '2015-07-04'
    for f in TEST_FILES:
        # read the file to process from command line
        dmp_file_path = TEST_DATA_DIR + 'data/' + f
        print dmp_file_path

        # test process 1 file
        print test_one_file_processing(None, dmp_file_path, get_stamp(dmp_file_path))
    '''
    # test one file

    dmp_file_path = TEST_DATA_DIR + 'data/' + TEST_FILES[14]
    print test_one_file_processing(None, dmp_file_path, get_stamp(dmp_file_path))
    #'''
    # tidy up
    clear_all_test_results(None)