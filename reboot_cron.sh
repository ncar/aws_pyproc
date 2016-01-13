#!/usr/bin/env bash

ROOT_DIR="/opt/processor/"

# check to see if reboot needed
UNPROC=`sh $ROOT_DIR"un.sh"`
if [ -n "$UNPROC" ]
then
	# ensure proc deamon stopped
	sh $ROOT_DIR"stop.sh"

	# start
	sh $ROOT_DIR"start.sh"

	echo "reprocessing unprocessed files in 3 secs..."
	sleep 3

	# get unprocessed files ready
	ROOT_DIR="/opt/processor/"
	rm -rf $ROOT_DIR"unproc"
	mkdir $ROOT_DIR"unproc"
	mv `sh $ROOT_DIR"un.sh"` $ROOT_DIR"unproc"

	# re-load the unprocessed files for processing
	FILES_DIR=$ROOT_DIR"unproc/"
	FILES=$FILES_DIR"*"
	for f in $FILES
	do
		echo "reprocessing  "$f
		cp $f /opt/data/incoming/
	done

	# tidy up copy of unprocessed files
	rm -r $ROOT_DIR"unproc"
	echo "completed reprocessing"
fi
