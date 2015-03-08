#!/usr/bin/env bash
#sudo kill `ps aux | grep procdeamon | grep -v "grep" | head -3 | awk '{print $2}'`
kill `more /opt/procdeamon/procdeamon.pid`
rm /opt/procdeamon/procdeamon.pid
echo "procdeamon stopped"