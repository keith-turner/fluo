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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.core.worker.finder.hash.ParitionManager.PartitionInfo;
import org.junit.Assert;
import org.junit.Test;

import static java.util.stream.Collectors.toList;

public class PartitionManagerTest {
  
  @Test
  public void testGrouping() {
    IntFunction<String> nff = i -> String.format("f-%04d",i);
    
    TreeSet<String> children = new TreeSet<>();
    
    IntStream.range(0, 20).mapToObj(nff).forEach(children::add);
    Collection<Bytes> rows = IntStream.range(0, 100).mapToObj(i -> String.format("r%06d",i)).map(Bytes::of).collect(toList());
    Collection<TabletRange> tablets = TabletRange.toTabletRanges(rows);
    
    Set<String> idCombos = new HashSet<>();
    
    
    for(int i = 0; i< 20; i++) {
      String me = nff.apply(i);
      PartitionInfo pi = ParitionManager.getGroupInfo(me, children, tablets, 5);
      Assert.assertEquals(4, pi.groups);
      Assert.assertEquals(5, pi.groupSize);
      Assert.assertEquals(20, pi.workers);
      Assert.assertTrue(pi.idInGroup >= 0 && pi.idInGroup < 5);
      Assert.assertTrue(pi.groupId >= 0 && pi.groupId < 4);
    
      Assert.assertFalse(idCombos.contains(pi.groupId+":"+pi.idInGroup));
      idCombos.add(pi.groupId+":"+pi.idInGroup);
      
      //TODO check tablets
      
      System.out.println(pi);
    }
    
    Assert.assertEquals(20, idCombos.size());
  }
}
