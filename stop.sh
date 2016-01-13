#!/usr/bin/env bash

# if there's a procdeamon.py process, kill it
PID=`ps aux | grep procdeamon | grep -v "grep" | head -3 | awk '{print $2}'`
if [ -z "$PID" ]
then
	echo "no procdeamon process to stop"
else
	kill $PID
	# if there's a PID file, remove it
	PID_FILE="/opt/processor/procdeamon.pid"
	if [ -f "$PID_FILE" ]
	then
	        rm $PID_FILE
	fi

	echo "procdeamon stopped"
fi

