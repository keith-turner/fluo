/*
 * Copyright 2015 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.core.impl;

import io.fluo.api.config.FluoConfiguration;
import io.fluo.core.worker.finder.hash.ScanTask;

/**
 * Contains implementation-related Fluo properties that should not be exposed in the API in
 * {@link FluoConfiguration}
 */
public class FluoConfigurationImpl {

  public static final String FLUO_IMPL_PREFIX = FluoConfiguration.FLUO_PREFIX + ".impl";

  public static final String ORACLE_PORT_PROP = FLUO_IMPL_PREFIX + ".oracle.port";
  public static final String WORKER_FINDER_PROP = FLUO_IMPL_PREFIX + ".worker.finder";
  public static final String METRICS_RESERVOIR_PROP = FLUO_IMPL_PREFIX + ".metrics.reservoir";
  public static final String MIN_SLEEP_TIME_PROP = FLUO_IMPL_PREFIX
      + ScanTask.class.getSimpleName() + ".minSleep";
  public static final int MIN_SLEEP_TIME_DEFAULT = 5000;
  public static final String MAX_SLEEP_TIME_PROP = FLUO_IMPL_PREFIX
      + ScanTask.class.getSimpleName() + ".maxSleep";
  public static final int MAX_SLEEP_TIME_DEFAULT = 5 * 60 * 1000;

  // Time period that each client will update ZK with their oldest active timestamp
  // If period is too short, Zookeeper may be overloaded. If too long, garbage collection
  // may keep older versions of table data unnecessarily.
  public static final String ZK_UPDATE_PERIOD_PROP = FLUO_IMPL_PREFIX + ".timestamp.update.period";
  public static long ZK_UPDATE_PERIOD_MS_DEFAULT = 60000;


  public static final String CW_MIN_THREADS_PROP = FLUO_IMPL_PREFIX + ".cw.threads.min";
  public static final int CW_MIN_THREADS_DEFAULT = 3;
  public static final String CW_MAX_THREADS_PROP = FLUO_IMPL_PREFIX + ".cw.threads.max";
  public static final int CW_MAX_THREADS_DEFAULT = 20;

  public static int getNumCWThreads(FluoConfiguration conf) {
    int min = conf.getInt(CW_MIN_THREADS_PROP, CW_MIN_THREADS_DEFAULT);
    int max = conf.getInt(CW_MAX_THREADS_PROP, CW_MAX_THREADS_DEFAULT);

    if (min < 0 || max < 0 || min > max) {
      throw new IllegalArgumentException("Bad conditional writer thread props " + min + " " + max);
    }

    int numWorkers = conf.getWorkerInstances();

    int numThreads = numWorkers / 2;
    numThreads = Math.min(numThreads, max);
    numThreads = Math.max(numThreads, min);

    return numThreads;
  }
}
