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

import java.util.function.Predicate;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.fluo.core.worker.TabletInfoCache.TabletInfo;
import org.apache.fluo.core.worker.finder.hash.ParitionManager.PartitionInfo;
import org.apache.hadoop.io.Text;

public class TabletPartitioner implements Predicate<TabletInfo<?>> {

  private PartitionInfo partition;

  TabletPartitioner(PartitionInfo pi) {
    this.partition = pi;
  }

  static void hash(Hasher hasher, Text t) {
    if (t == null) {
      hasher.putBytes(new byte[0]);
    } else {
      hasher.putBytes(t.getBytes(), 0, t.getLength());
    }
  }

  @Override
  public boolean test(TabletInfo<?> t) {
    Hasher hasher = Hashing.murmur3_32().newHasher();

    hash(hasher, t.getStart());
    hash(hasher, t.getEnd());

    int hc = Math.abs(hasher.hashCode());

    // TODO what about group size of zero?
    return hc % partition.groupSize == partition.idInGroup;
  }

}
