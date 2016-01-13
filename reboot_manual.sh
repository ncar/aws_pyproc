#!/usr/bin/env bash

# ensure proc deamon stopped
sh /opt/processor/stop.sh

# start
sh /opt/processor/start.sh

echo "reprocessing unprocessed files in 3 secs..."
sleep 3

# get unprocessed files ready
UNPROC=`sh un.sh`
if [ -z "$UNPROC" ]
then
        echo "no unprocessed files"
else
	ROOT_DIR="/opt/processor/"
	rm -rf unproc
	mkdir unproc
	mv `sh un.sh` unproc

	# re-load the unprocessed files for processing
	FILES_DIR="unproc/"
	FILES=$ROOT_DIR$FILES_DIR"*"
	for f in $FILES
	do
		echo "reprocessing  "$f
		cp $f /opt/data/incoming/
	done

	# tidy up copy of unprocessed files
	rm -r unproc
fi
echo "completed reprocessing"
