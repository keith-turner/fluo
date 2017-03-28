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
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.accumulo.core.data.Range;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.core.util.ByteUtil;

import static java.util.stream.Collectors.toList;

public class TabletRange {
  private final Bytes prevEndRow;
  private final Bytes endRow;
  private final int hc;

  public TabletRange(Bytes per, Bytes er) {
    this.prevEndRow = per;
    this.endRow = er;

    Hasher hasher = Hashing.murmur3_32().newHasher();
    if (per != null) {
      hasher.putBytes(per.toArray());
    }

    if (er != null) {
      hasher.putBytes(er.toArray());
    }

    this.hc = hasher.hash().asInt();
  }

  @Override
  public int hashCode() {
    return hc;
  }

  public int persistentHashCode() {
    return hc;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof TabletRange) {
      TabletRange ot = (TabletRange) o;
      return Objects.equals(prevEndRow, ot.prevEndRow) && Objects.equals(endRow, ot.endRow);
    }

    return false;
  }

  public Bytes getPrevEndRow() {
    return prevEndRow;
  }

  public Bytes getEndRow() {
    return endRow;
  }

  public boolean contains(Bytes row) {
    return (prevEndRow == null || row.compareTo(prevEndRow) > 0)
        && (endRow == null || row.compareTo(endRow) <= 0);
  }

  @Override
  public String toString() {
    return getPrevEndRow() + " " + getEndRow();
  }


  public static Collection<TabletRange> toTabletRanges(Collection<Bytes> rows) {
    List<Bytes> sortedRows = rows.stream().sorted().collect(toList());
    List<TabletRange> tablets = new ArrayList<>(sortedRows.size() + 1);
    for (int i = 0; i < sortedRows.size(); i++) {
      tablets.add(new TabletRange(i == 0 ? null : sortedRows.get(i - 1), sortedRows.get(i)));
    }

    tablets.add(new TabletRange(sortedRows.size() == 0 ? null
        : sortedRows.get(sortedRows.size() - 1), null));
    return tablets;
  }

  public Range getRange() {
    return new Range(ByteUtil.toText(prevEndRow), false, ByteUtil.toText(endRow), true);
  }
}
