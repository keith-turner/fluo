/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fluo.core.worker.finder.hash;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.accumulo.iterators.NotificationHashFilter;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.FluoConfigurationImpl;
import org.apache.fluo.core.impl.Notification;
import org.apache.fluo.core.util.UtilWaitThread;
import org.apache.fluo.core.worker.NotificationFinder;
import org.apache.fluo.core.worker.NotificationProcessor;
import org.apache.fluo.core.worker.finder.hash.ParitionManager.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanTask implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(ScanTask.class);

  private final NotificationFinder finder;
  private final ParitionManager partitionManager;
  private final NotificationProcessor proccessor;
  private final Random rand = new Random();
  private final AtomicBoolean stopped;
  private final Map<TabletRange, TabletData> tabletsData;
  private final Environment env;

  static long STABILIZE_TIME = 10 * 1000;

  private long minSleepTime;
  private long maxSleepTime;

  ScanTask(NotificationFinder finder, NotificationProcessor proccessor,
      ParitionManager partitionManager, Environment env, AtomicBoolean stopped) {
    this.finder = finder;
    this.tabletsData = new HashMap<>();

    this.env = env;
    this.stopped = stopped;

    this.proccessor = proccessor;
    this.partitionManager = partitionManager;

    minSleepTime =
        env.getConfiguration().getInt(FluoConfigurationImpl.MIN_SLEEP_TIME_PROP,
            FluoConfigurationImpl.MIN_SLEEP_TIME_DEFAULT);
    maxSleepTime =
        env.getConfiguration().getInt(FluoConfigurationImpl.MAX_SLEEP_TIME_PROP,
            FluoConfigurationImpl.MAX_SLEEP_TIME_DEFAULT);
  }

  @Override
  public void run() {


    List<TabletRange> tablets = new ArrayList<>();
    Set<TabletRange> tabletSet = new HashSet<>();

    int qSize = proccessor.size();

    while (!stopped.get()) {
      try {
        System.out.println("Waiting for partition info ");

        PartitionInfo partition = partitionManager.waitForPartitionInfo();

        System.out.println("Got partition info " + partition);

        while (proccessor.size() > qSize / 2 && !stopped.get()) {
          UtilWaitThread.sleep(50, stopped);
        }

        tablets.clear();
        tabletSet.clear();
        partition.groupsTablets.forEach(t -> {
          tablets.add(t);
          tabletSet.add(t);
        });
        Collections.shuffle(tablets, rand);
        tabletsData.keySet().retainAll(tabletSet);

        long minRetryTime = maxSleepTime + System.currentTimeMillis();
        int notifications = 0;
        int tabletsScanned = 0;
        try {
          for (TabletRange tabletRange : tablets) {

            System.out.println("scanning tablet " + tabletRange);

            TabletData tabletData =
                tabletsData.computeIfAbsent(tabletRange, tr -> new TabletData());

            int count = 0;
            PartitionInfo pi = partitionManager.getPartitionInfo();
            if (partition.equals(pi)) {
              try {
                // TODO following prevents adding notifications with a later date....
                // start remembering any deleted notifications for this tablet... will not add them
                // back
                proccessor.beginAddingNotifications(rc -> tabletRange.contains(rc.getRow()));
                // notifications could have been asynchronously queued for deletion. Let that happen
                // 1st before scanning
                env.getSharedResources().getBatchWriter().waitForAsyncFlush(); // TODO think about
                                                                               // order of these

                count = scan(partition, tabletRange.getRange());
                tabletsScanned++;
              } finally {
                proccessor.finishAddingNotifications();
              }
            } else {
              break;
            }
            tabletData.updateScanCount(count, maxSleepTime);
            notifications += count;
            if (stopped.get()) {
              break;
            }

            minRetryTime = Math.min(tabletData.retryTime, minRetryTime);
          }
        } catch (PartitionInfoChangedException mpce) {
        }

        long sleepTime = Math.max(minSleepTime, minRetryTime - System.currentTimeMillis());

        qSize = proccessor.size();

        log.debug("Scanned {} of {} tablets, added {} new notifications (total queued {})",
            tabletsScanned, tablets.size(), notifications, qSize);

        if (!stopped.get()) {
          UtilWaitThread.sleep(sleepTime, stopped);
        }

      } catch (Exception e) {
        if (isInterruptedException(e)) {
          log.debug("Error while looking for notifications", e);
        } else {
          log.error("Error while looking for notifications", e);
        }
      }
    }
  }

  private boolean isInterruptedException(Exception e) {
    boolean wasInt = false;
    Throwable cause = e;
    while (cause != null) {
      if (cause instanceof InterruptedException) {
        wasInt = true;
      }
      cause = cause.getCause();
    }
    return wasInt;
  }

  private int scan(PartitionInfo pi, Range range) throws TableNotFoundException {
    Scanner scanner = env.getConnector().createScanner(env.getTable(), env.getAuthorizations());

    scanner.setRange(range);

    Notification.configureScanner(scanner);

    IteratorSetting iterCfg = new IteratorSetting(30, "nhf", NotificationHashFilter.class);
    NotificationHashFilter.setModulusParams(iterCfg, pi.groupSize, pi.idInGroup);
    scanner.addScanIterator(iterCfg);

    int count = 0;

    for (Entry<Key, Value> entry : scanner) {
      if (!pi.equals(partitionManager.getPartitionInfo())) {
        throw new PartitionInfoChangedException();
      }

      if (stopped.get()) {
        return count;
      }

      if (proccessor.addNotification(finder, Notification.from(entry.getKey()))) {
        count++;
      }
    }
    return count;
  }
}
