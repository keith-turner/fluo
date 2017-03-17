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

import java.util.function.BiConsumer;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.metrics.MetricsReporter;
import org.apache.fluo.api.observer.Observer.NotificationType;

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
     * @return A {@link MetricsReporter} to report application metrics from observers.
     */
    MetricsReporter getMetricsReporter();
  }

  /**
   * @since 1.1.0
   */
  interface ObserverConsumer {
    void accept(Column observedColumn, NotificationType ntfyType, Observer observer);

    void accepts(Column observedColumn, NotificationType ntfyType, StringObserver observer);
  }

  void createObservers(ObserverConsumer obsConsumer, Context ctx);

  default void getObservedColumns(Context ctx, BiConsumer<Column, NotificationType> obsColConsumer) {
    ObserverConsumer obsConsumer = new ObserverConsumer() {
      @Override
      public void accepts(Column oc, NotificationType nt, StringObserver obs) {
        obsColConsumer.accept(oc, nt);
      }

      @Override
      public void accept(Column oc, NotificationType nt, Observer obs) {
        obsColConsumer.accept(oc, nt);
      }
    };

    createObservers(obsConsumer, ctx);
  }
}
