import settings
import functions
import MySQLdb
import logging
import sys
from operator import sub


def get_results_from_tbls(conn, aws_id, table):
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
        AND DATE(stamp) = '2015-07-04'
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


def test_one_file_processing(conn, dmp_file_path):
    # get station details
    [aws_id, scm] = functions.get_station_details(conn, dmp_file_path)

    # process the file
    functions.process(dmp_file_path)

    # compare the processed file's results to the test results
    a = get_results_from_tbls(conn, aws_id, 'tbl_15min_test')
    print a
    b = get_results_from_tbls(conn, aws_id, 'tbl_data_minutes')
    print b

    return results_are_same(compare_table_results(a, b))


def compare_table_results(tbl_15min_test_results, tbl_data_minutes_results):
    return tuple(map(sub, tbl_15min_test_results, tbl_data_minutes_results))


def results_are_same(compare_result):
    guestimate = 0
    for val in compare_result:
        guestimate += val

    if guestimate < 1:
        return True
    else:
        return False


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
        'ALDINGA150704-0000.DMP',  # pass
        'CADELL150704-0000.DMP',  # pass
        'LEWIS150704-0000.DMP',  # fail - error
        'NARRUNG150704-0000.DMP',  # pass
        'SandyBor150704-0000.DMP',  # fail - numbers
        'SEAVIEW150704-0000.DMP',  # pass
        'SELLICKS150704-0000.DMP',  # pass
        'Wellngtn150704-0000.DMP',  # pass
        'Wemen150704-0000.DMP',  # pass
        'SYMON150704-0000.DMP',  # pass
        'Watarru150704-0000.DMP',  # fail - numbers
    ]

    # read the file to process from command line
    dmp_file_path = TEST_DATA_DIR + 'data/' + TEST_FILES[10]

    print dmp_file_path
    # test process 1 file
    print test_one_file_processing(None, dmp_file_path)



    clear_all_test_results(None)