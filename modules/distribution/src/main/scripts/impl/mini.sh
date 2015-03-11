#! /usr/bin/env bash

# Copyright 2014 Fluo authors (see AUTHORS)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Start: Resolve Script Directory
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
   impl="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
   SOURCE="$(readlink "$SOURCE")"
   [[ $SOURCE != /* ]] && SOURCE="$impl/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
impl="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
script=$( basename "$SOURCE" )
# Stop: Resolve Script Directory

. "$impl"/config.sh

LOGHOST=$(hostname)
SERVICE="mini"
MINI_OPTS="-config-dir $FLUO_CONF_DIR -log-output $FLUO_LOG_DIR"
MINI_OUT=${FLUO_LOG_DIR}/${SERVICE}_${LOGHOST}.out
MINI_ERR=${FLUO_LOG_DIR}/${SERVICE}_${LOGHOST}.err
MINI_LIB="$FLUO_LIB_DIR/client/*:$FLUO_LIB_DIR/log4j/*"

case "$1" in
start)
  echo -n "Starting MiniFluo..."
  java -cp "$MINI_LIB:$FLUO_LIB_DIR/logback/*:$FLUO_LIB_DIR/observers/*" io.fluo.cluster.mini.MiniFluoMain $MINI_OPTS >$MINI_OUT 2>$MINI_ERR &
  echo "DONE"
	;;
stop)
  echo -n "Stopping MiniFluo..."
	kill `jps -m | grep MiniFluoMain | cut -f 1 -d ' '`
  java -cp "$MINI_LIB" io.fluo.cluster.mini.MiniAdmin -config-dir $FLUO_CONF_DIR -command stop
  echo "DONE"
	;;
*)
	echo -e "Usage: fluo mini <argument>\n"
  echo -e "Possible arguments:\n"
  echo "  start       Starts MiniFluo instance on local machine"
  echo "  stop        Stops MiniFluo instance on local machine"
  echo " " 
  exit 1
esac
