#!/usr/bin/python

import sys
from subprocess import Popen, PIPE, call
import csv
import random
import time

while True:
  proc = Popen("./bin/fluo-dev fluo yarn csv", shell=True, bufsize=1, stdout=PIPE)
  csv_reader = csv.DictReader(proc.stdout)

  workers=[]

  for row in csv_reader:
    if row['fluo_type'] == 'worker':
      workers.append(row)
  proc.wait()

  worker = random.choice(workers)
  print "Killing "+worker['container_id']+" "+worker['host']

  call(['ssh',worker['host'],"pkill -9 -f "+worker['container_id']]) 

  time.sleep(120)

