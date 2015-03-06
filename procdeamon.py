import sys
import pyinotify
import os
import functions.processor as processor
import logging
import ftplib
import settings


class MyEventHandler(pyinotify.ProcessEvent):
    def my_init(self, file_object=sys.stdout):
        self._file_object = file_object
        logging.info("procdeamon Turned On")

    #react when a file is created
    def process_IN_CLOSE_WRITE(self, event):
        try:
            p = event.pathname
            # only notice files with .DMP at end
            if not os.path.isfile(p) and p[-3:] != "DMP":
                logging.info("Ignored, not DMP file: " + p)
                return

            # delete zero-length files
            if os.stat(p).st_size == 0:
                logging.info("Removing, file size is 0: " + p)
                os.unlink(p)
                return

            # string {filename}.DMP
            file_name = p.split('/')[-1]
            [aws_id, scm_file] = processor.get_station_details(p)
            # don't process unknown station's data
            if aws_id is None or len(aws_id) == 0:
                logging.info("Unprocessed, station not known: " + p)
                return

            #read data from the file that arrived
            fin = open(p, "rb")
            data = fin.read()
            fin.close()

            #check if archiving folder for this file exists
            if not os.path.exists(settings.ARCHIVE_DIR + aws_id):
                os.makedirs(settings.ARCHIVE_DIR + aws_id)

            #
            #   Process DMP file
            #
            #sql = processor.process_multi_file(p)

            # do anything else that needs doing, based on aws_id
            additional_handling(aws_id, p)

            #
            #   Archive the DMP data
            #
            #copy/append processed file to station's home dir then delete the file that arrived
            fout = open(settings.ARCHIVE_DIR + aws_id + '/' + file_name, "ab")
            fout.write(data)
            fout.close()
            os.remove(p)

            logging.info("Processed: " + p)

        except IndexError, e:
            logging.error("procdeamon IN_CREATE index error " + p + "\nError report: " + str(e))
            #print traceback.format_exc()
        except Exception, e:
            logging.error("procdeamon IN_CREATE " + p + "\nError report: " + str(e))
            #emailerror.send("procdeamon IN_CREATE " + p + "\nError report: " + str(e))

        return


def additional_handling(aws_id, p):
    # currently the only additional task is to FTP the two Adelaide station's data to Adelaide NRM's servers
    if aws_id == 'ADLD01' or aws_id == 'ADLD02':
        ftp_file_to_adld(aws_id, p)


#
#	FTP for Charleston & Virginia to Ad MtLR
#
def ftp_file_to_adld(aws_id, path):
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


if __name__ == '__main__':
    logging.basicConfig(filename=settings.LOG_FILE,
                        format='%(asctime)s %(levelname)s %(message)s',
                        level=settings.LOG_LEVEL)

    try:
        wm = pyinotify.WatchManager()
        notifier = pyinotify.Notifier(wm, MyEventHandler())
        # Exclude patterns from list: i.e. the old folder
        excl = pyinotify.ExcludeFilter(['^/home/ftp/aws/old/'])
        wm.add_watch(settings.WATCHED_DIR, pyinotify.IN_CLOSE_WRITE, rec=True, exclude_filter=excl)
        notifier.loop(
            daemonize=True,
            pid_file=settings.PID_FILE,
            stdout=settings.STD_OUT_LOG)
    except pyinotify.NotifierError, err:
        logging.error(err)
