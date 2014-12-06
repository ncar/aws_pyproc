import sys
import logging
import functions
import json


if __name__ == "__main__":
    #set up logging
    logging.basicConfig(filename='saaws.log',
                        format='%(asctime)s %(levelname)s\n%(message)s\n\n',
                        level=logging.DEBUG)

    #dmp_file = sys.argv[1]
    #dmp_file = '/var/lib/saaws/data/CADELL140901-0000.DMP'
    #dmp_file = '/var/lib/saaws/data/ROBY140901-0000.DMP'
    #dmp_file = '/var/lib/saaws/data/STIRLING140901-0000.DMP'
    #dmp_file = '/var/lib/saaws/data/Murputja140901-0000.DMP'
    dmp_file = 'C:\\Users\\car587\\Desktop\\WindDir\\STIRLING140901-0000.DMP'
    logging.debug('processing file ' + dmp_file)

    conn = functions.connect_to_aws_db(host="aws-samdbnrm.sa.gov.au", user="aws", passwd="ascetall", db="aws")

    [aws_id, scm_doc] = functions.get_station_details(conn, dmp_file)
    reading_vars = functions.reading_vars_from_scm(scm_doc)
    #for var in reading_vars:
    #    print var

    #print '-----------------------------------------------------'
    enhanced_reading_vars = functions.readings_vars_additions_from_db(conn, reading_vars)
    #for var in enhanced_reading_vars:
    #    print var

    dumps = functions.get_dmp_file_dumps(dmp_file)

    sql = ''
    for dump in dumps:
        sql += functions.generate_insert_sql(aws_id, functions.parse_dump(dump, enhanced_reading_vars)) + '\n'

    print sql
    #insert = functions.insert_reading(sql)
    insert = True
    if insert:
        print 'ok'
    else:
        print insert[1]


    #with open('results.json', 'w') as outfile:
    #    json.dump(results, outfile, indent=4)



    #buffers = functions.get_dmp_file_dumps(sys.argv[1])
    #for buffer in buffers:
    #    print buffer



    #for buffer in buffers:
    #    print functions.parse_reading_timestamps(buffer)

    #with open('data/STIRLING.scm', "r") as f:
    #    scm_xml = f.read().replace('\n', '')

