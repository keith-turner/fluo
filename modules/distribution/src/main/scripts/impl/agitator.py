#!/usr/bin/python

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


import sys
from subprocess import Popen, PIPE, call
import csv
import random
import time

if len(sys.argv) != 3:
  print "Usage : "+sys.argv[0]+" <num to kill> <sleep time>"
  sys.exit(1)

numToKill=sys.argv[1]
sleepTime=sys.argv[2]

while True:
  proc = Popen("fluo yarn csv", shell=True, bufsize=1, stdout=PIPE)
  csv_reader = csv.DictReader(proc.stdout)

  workers=[]

  for row in csv_reader:
    if row['fluo_type'] == 'worker':
      workers.append(row)
  proc.wait()

  for i in range(0, int(numToKill)):
    worker = random.choice(workers)
    print "Killing "+worker['container_id']+" "+worker['host']
    cid=worker['container_id']
    cid=cid[:-1]+'['+cid[-1]+']'
    call(['ssh',worker['host'],"pkill -9 -f "+cid])
    workers.remove(worker)

  time.sleep(float(sleepTime))

