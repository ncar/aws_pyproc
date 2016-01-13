#!/usr/bin/env bash

# only start if there is not already a process running
PID=`ps aux | grep procdeamon | grep -v "grep" | head -3 | awk '{print $2}'`
if [ -z "$PID" ]
then
	python /opt/processor/procdeamon.py &
	echo "procdeamon started..."
else
        echo "procdeamon already running"
fi

