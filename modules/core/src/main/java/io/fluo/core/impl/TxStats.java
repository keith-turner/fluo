/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fluo.core.impl;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import io.fluo.api.config.FluoConfiguration;

public class TxStats {
  private final long startTime;
  private long lockWaitTime = 0;
  private long entriesReturned = 0;
  private long entriesSet = 0;
  private long finishTime = 0;
  private long collisions = 0;
  // number of entries recovered from other transactions
  private long recovered = 0;
  private long deadLocks = 0;
  private long timedOutLocks = 0;

  TxStats() {
    this.startTime = System.currentTimeMillis();
  }

  public long getLockWaitTime() {
    return lockWaitTime;
  }

  public long getEntriesReturned() {
    return entriesReturned;
  }

  public long getEntriesSet() {
    return entriesSet;
  }

  public long getTime() {
    return finishTime - startTime;
  }

  public long getCollisions() {
    return collisions;
  }

  public long getRecovered() {
    return recovered;
  }
  
  public long getDeadLocks() {
    return deadLocks;
  }
  
  public long getTimedOutLocks() {
    return timedOutLocks;
  }

  void incrementLockWaitTime(long l) {
    lockWaitTime += l;
  }

  void incrementEntriesReturned(long l) {
    entriesReturned += l;
  }

  void incrementEntriesSet(long l) {
    entriesSet += l;
  }

  void incrementCollisions(long c) {
    collisions += c;
  }
  
  void incrementDeadLocks() {
    deadLocks++;
  }
  
  void incrementTimedOutLocks() {
    timedOutLocks++;
  }

  void incrementTimedOutLocks(int amt) {
    timedOutLocks += amt;
  }

  void setFinishTime(long t) {
    finishTime = t;
  }

  public void report(String status, Class<?> execClass, MetricRegistry registry) {
    String sn = execClass.getSimpleName();
    String prefix = FluoConfiguration.FLUO_PREFIX + ".tx.";
    registry.timer(prefix + "lockWait." + sn).update(getLockWaitTime(), TimeUnit.MILLISECONDS);
    registry.timer(prefix + "time." + sn).update(getTime(), TimeUnit.MILLISECONDS);
    registry.counter(prefix + "collisions." + sn).inc(getCollisions());
    registry.counter(prefix + "set." + sn).inc(getEntriesSet());
    registry.counter(prefix + "read." + sn).inc(getEntriesReturned());
    registry.counter(prefix + "locks.timedout." + sn).inc(getTimedOutLocks());
    registry.counter(prefix + "locks.dead." + sn).inc(getDeadLocks());
    registry.counter(prefix + "status." + status.toLowerCase() + "." + sn).inc();
  }
}
