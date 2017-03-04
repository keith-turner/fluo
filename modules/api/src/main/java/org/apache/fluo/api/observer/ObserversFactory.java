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

package org.apache.fluo.api.observer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.metrics.MetricsReporter;
import org.apache.fluo.api.observer.Observer.NotificationType;
import org.apache.fluo.api.observer.Observer.ObservedColumn;

/**
 * @since 1.1.0
 */
public interface ObserversFactory {

  /**
   * @since 1.1.0
   */
  interface Context {
    /**
     * @return A configuration object with application configuration like that returned by
     *         {@link FluoClient#getAppConfiguration()}
     */
    SimpleConfiguration getAppConfiguration();

    /**
     * @return A {@link MetricsReporter} to report application metrics from this observer
     */
    MetricsReporter getMetricsReporter();
  }

  /**
   * @since 1.1.0
   */
  interface ObserverConsumer {
    void accept(Column observedColumn, NotificationType ntfyType, Observer observer);
  }

  void createObservers(ObserverConsumer consumer, Context ctx);

  default Collection<ObservedColumn> getObservedColumns(Context ctx) {
    HashSet<Column> columnsSeen = new HashSet<>();
    List<ObservedColumn> observedColumns = new ArrayList<>();
    ObserverConsumer obsConsumer = (oc, nt, obs) -> {
      Preconditions.checkArgument(!columnsSeen.contains(oc), "Duplicate observed column : %s", oc);
      columnsSeen.add(oc);
      observedColumns.add(new ObservedColumn(oc, nt));
    };

    createObservers(obsConsumer, ctx);

    return observedColumns;
  }
}
