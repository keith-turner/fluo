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

package org.apache.fluo.core.observer;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.Observer;
import org.apache.fluo.api.observer.Observer.NotificationType;
import org.apache.fluo.api.observer.ObserversFactory.ObserverConsumer;

public class ObserverConsumerImpl implements ObserverConsumer {

  public static class ObserverValue {
    public final Observer observer;
    public final NotificationType ntfyType;

    public ObserverValue(Observer observer, NotificationType ntfyType) {
      this.observer = observer;
      this.ntfyType = ntfyType;
    }
  }

  private Map<Column, ObserverValue> observers = new HashMap<>();

  @Override
  public void accept(Column observedColumn, NotificationType ntfyType, Observer observer) {
    Preconditions.checkArgument(!observers.containsKey(observedColumn),
        "Duplicate observed column : %s", observedColumn);
    observers.put(observedColumn, new ObserverValue(observer, ntfyType));
  }


}
