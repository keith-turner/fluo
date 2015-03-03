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

# Sets HADOOP_PREFIX if it is not already set.  Please modify the 
# export statement to use the correct directory.  Remove the test
# statement to override any previously set environment.
test -z "$HADOOP_PREFIX" && export HADOOP_PREFIX=/path/to/hadoop


test -z "$ACCUMULO_HOME" && ACCUMULO_HOME=/opt/accumulo
test -z "$ZOOKEEPER_HOME" && ZOOKEEPER_HOME=/opt/zookeeper

ACCUMULO_CLASSPATH="$ACCUMULO_HOME/lib/*"
HADOOP_CLASSPATH=`$HADOOP_PREFIX/bin/hadoop classpath`
ZOOKEEPER_CLASSPATH="$ZOOKEEPER_HOME/*"

#set FLUO_DEPENDENCIES_CLASSPATH to use dependencies installed on system.  otherwise fluo will use $FLUO_HOME/lib/dependencies
FLUO_DEPENDENCIES_CLASSPATH="$ACCUMULO_CLASSPATH:$ZOOKEEPER_CLASSPATH:$HADOOP_CLASSPATH"

