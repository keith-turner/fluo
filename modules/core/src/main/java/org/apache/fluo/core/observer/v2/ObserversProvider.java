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

package org.apache.fluo.core.observer.v2;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.api.observer.Observer;
import org.apache.fluo.api.observer.Observer.NotificationType;
import org.apache.fluo.api.observer.ObserversFactory;
import org.apache.fluo.api.observer.ObserversFactory.ObserverConsumer;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.observer.ObserverProvider;

class ObserversProvider implements ObserverProvider {

  Map<Column, Observer> observers;

  public ObserversProvider(Environment env, JsonObservers jco, Set<Column> strongColumns,
      Set<Column> weakColumns) {
    observers = new HashMap<>();

    ObserversFactory obsFact = ObserversV2.newObserversFactory(jco.getObserversFactoryClass());

    ObserverFactoryContextImpl ctx = new ObserverFactoryContextImpl(env);

    ObserverConsumer consumer = (col, nt, obs) -> {
      // TODO maybe warn if has close method
        if (nt == NotificationType.STRONG && !strongColumns.contains(col)) {
          throw new IllegalArgumentException("Column " + col
              + " not previously configured for strong notifications");
        }

        if (nt == NotificationType.WEAK && !weakColumns.contains(col)) {
          throw new IllegalArgumentException("Column " + col
              + " not previously configured for weak notifications");
        }

        observers.put(col, obs);
      };
    obsFact.createObservers(consumer, ctx);

    // the following checks ensure the observers factory provides observers for all previously
    // configured columns

    // TODO this may be inefficient
    SetView<Column> diff =
        Sets.difference(observers.keySet(), Sets.union(strongColumns, weakColumns));
    if (diff.size() > 0) {
      throw new FluoException("ObserversFactory " + jco.getObserversFactoryClass()
          + " did not provide observers for columns " + diff);
    }
  }

  @Override
  public Observer getObserver(Column col) {
    return observers.get(col);
  }

  @Override
  public void returnObserver(Observer o) {}

  @Override
  public void close() {}

}
