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

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.Observer.NotificationType;
import org.apache.fluo.api.observer.Observer.ObservedColumn;

// this class created for json serialization
class JsonObservedColumn {
  private byte[] fam;
  private byte[] qual;
  private byte[] vis;
  private String notificationType;

  JsonObservedColumn(ObservedColumn oc) {
    this.fam = oc.getColumn().getFamily().toArray();
    this.qual = oc.getColumn().getQualifier().toArray();
    this.vis = oc.getColumn().getVisibility().toArray();
    this.notificationType = oc.getType().name();
  }

  public ObservedColumn getObservedColumn() {
    Column col = new Column(Bytes.of(fam), Bytes.of(qual), Bytes.of(vis));
    return new ObservedColumn(col, NotificationType.valueOf(notificationType));
  }
}
