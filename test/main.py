import logging
import functions
import os
import settings


if __name__ == "__main__":
    # set up test logging
    logging.basicConfig(filename=settings.HOME_DIR + 'test/test.log',
                        format='%(asctime)s %(levelname)s\n%(message)s\n\n',
                        level=logging.DEBUG)

    # get test data files
    dmp_files = []
    dmp_files_dir = settings.HOME_DIR + 'test/DMP_files/'
    for f in os.listdir(dmp_files_dir):
        dmp_files.append(dmp_files_dir + f)
        functions.process(dmp_files_dir + f)


    #functions.process(dmp_files_dir + 'NENANDI150306-0000.DMP')
    #functions.process(dmp_files_dir + 'ALDINGA150306-0100.DMP')
    #functions.process(dmp_files_dir + 'MTCMPASS150306-0000.DMP')
    #functions.process(dmp_files_dir + 'JOYCE150306-0000.DMP')
    #functions.process(dmp_files_dir + 'CADELL150306-0000.DMP')


    #functions.process()
    #for var in reading_vars:
    #    print var

    #print '-----------------------------------------------------'

    #for var in enhanced_reading_vars:
    #    print var
    '''
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
    '''
