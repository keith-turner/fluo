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

import java.util.Collection;
import java.util.List;

import org.apache.fluo.api.observer.Observer.ObservedColumn;

import static java.util.stream.Collectors.toList;

// this class created for json serialization
class JsonObservers {
  String obsFactoryClass;
  List<JsonObservedColumn> observedColumns;

  JsonObservers(String obsFactoryClass, Collection<ObservedColumn> columns) {
    this.obsFactoryClass = obsFactoryClass;
    this.observedColumns = columns.stream().map(JsonObservedColumn::new).collect(toList());
  }

  public String getObserversFactoryClass() {
    return obsFactoryClass;
  }

  public Collection<ObservedColumn> getObservedColumns() {
    return observedColumns.stream().map(JsonObservedColumn::getObservedColumn).collect(toList());
  }

  @Override
  public String toString() {
    return obsFactoryClass + " " + getObservedColumns();
  }
}
