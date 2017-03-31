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

import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.apache.fluo.api.data.Bytes;

public class TabletSet {
  private TreeMap<Bytes, TabletRange> tmap;
  private TabletRange lastTablet;

  public TabletSet(List<TabletRange> tablets) {
    tmap = new TreeMap<>();

    for (TabletRange tablet : tablets) {
      if (tablet.getEndRow() == null) {
        lastTablet = tablet;
      } else {
        tmap.put(tablet.getEndRow(), tablet);
      }
    }
  }

  public TabletRange getContaining(Bytes row) {
    Entry<Bytes, TabletRange> entry = tmap.ceilingEntry(row);
    if (entry != null) {
      if (entry.getValue().contains(row)) {
        return entry.getValue();
      }
    } else if (lastTablet != null) {
      if (lastTablet.contains(row)) {
        return lastTablet;
      }
    }

    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof TabletSet) {
      TabletSet ots = (TabletSet) o;
      
      if(tmap.size() != ots.tmap.size()) {
        return false;
      }
      
      for (Entry<Bytes,TabletRange> entry : tmap.entrySet()) {
        TabletRange otr = ots.tmap.get(entry.getKey());
        if(!Objects.equals(entry.getValue(), otr)){
          return false;
        }
      }
      
      return lastTablet.equals(ots.lastTablet);
    }
    return false;
  }

  public int size() {
    return tmap.size() + (lastTablet == null ? 0 : 1);
  }

  public void forEach(Consumer<TabletRange> trc) {
    if (lastTablet != null) {
      trc.accept(lastTablet);
    }
    tmap.values().forEach(trc);
  }
}
