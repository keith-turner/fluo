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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import org.apache.accumulo.core.data.Range;
import org.apache.fluo.api.data.Bytes;
import org.junit.Assert;
import org.junit.Test;

public class TabletRangeTest {
  @Test
  public void testBasic() {
    TabletRange tr1 = new TabletRange(null, null);

    Assert.assertTrue(tr1.contains(Bytes.of("a")));
    Assert.assertTrue(tr1.contains(Bytes.of("z")));
    Assert.assertNull(tr1.getEndRow());
    Assert.assertNull(tr1.getPrevEndRow());

    TabletRange tr2 = new TabletRange(null, Bytes.of("ma"));
    Assert.assertTrue(tr2.contains(Bytes.of("a")));
    Assert.assertTrue(tr2.contains(Bytes.of("ma")));
    Assert.assertFalse(tr2.contains(Bytes.of("maa")));
    Assert.assertFalse(tr2.contains(Bytes.of("z")));
    Assert.assertNull(tr2.getPrevEndRow());
    Assert.assertEquals(Bytes.of("ma"), tr2.getEndRow());

    TabletRange tr3 = new TabletRange(Bytes.of("la"), null);
    Assert.assertFalse(tr3.contains(Bytes.of("a")));
    Assert.assertFalse(tr3.contains(Bytes.of("la")));
    Assert.assertTrue(tr3.contains(Bytes.of("laa")));
    Assert.assertTrue(tr3.contains(Bytes.of("z")));
    Assert.assertEquals(Bytes.of("la"), tr3.getPrevEndRow());
    Assert.assertNull(tr3.getEndRow());

    TabletRange tr4 = new TabletRange(Bytes.of("la"), Bytes.of("ma"));
    Assert.assertFalse(tr4.contains(Bytes.of("a")));
    Assert.assertFalse(tr4.contains(Bytes.of("la")));
    Assert.assertTrue(tr4.contains(Bytes.of("laa")));
    Assert.assertTrue(tr4.contains(Bytes.of("ma")));
    Assert.assertFalse(tr4.contains(Bytes.of("maa")));
    Assert.assertFalse(tr4.contains(Bytes.of("z")));
    Assert.assertEquals(Bytes.of("la"), tr4.getPrevEndRow());
    Assert.assertEquals(Bytes.of("ma"), tr4.getEndRow());
  }

  @Test
  public void testMultiple() {

    Bytes sp1 = Bytes.of("e1");
    Bytes sp2 = Bytes.of("m1");
    Bytes sp3 = Bytes.of("r1");

    Collection<TabletRange> trc1 =
        new HashSet<>(TabletRange.toTabletRanges(Arrays.asList(sp2, sp3, sp1)));

    Assert.assertEquals(4, trc1.size());
    Assert.assertTrue(trc1.contains(new TabletRange(null, sp1)));
    Assert.assertTrue(trc1.contains(new TabletRange(sp1, sp2)));
    Assert.assertTrue(trc1.contains(new TabletRange(sp2, sp3)));
    Assert.assertTrue(trc1.contains(new TabletRange(sp3, null)));

    Collection<TabletRange> trc2 =
        new HashSet<>(TabletRange.toTabletRanges(Collections.emptyList()));
    Assert.assertEquals(1, trc2.size());
    Assert.assertTrue(trc2.contains(new TabletRange(null, null)));
  }

  @Test
  public void testCompare() {

    Bytes sp1 = Bytes.of("e1");
    Bytes sp2 = Bytes.of("m1");

    TabletRange tr1 = new TabletRange(null, sp1);
    TabletRange tr2 = new TabletRange(sp1, sp2);
    TabletRange tr3 = new TabletRange(sp2, null);

    Assert.assertTrue(tr1.compareTo(tr2) < 0);
    Assert.assertTrue(tr2.compareTo(tr1) > 0);

    Assert.assertTrue(tr2.compareTo(tr3) < 0);
    Assert.assertTrue(tr3.compareTo(tr2) > 0);

    Assert.assertTrue(tr1.compareTo(tr3) < 0);
    Assert.assertTrue(tr3.compareTo(tr1) > 0);

    Assert.assertTrue(tr1.compareTo(tr1) == 0);
    Assert.assertTrue(tr2.compareTo(tr2) == 0);
    Assert.assertTrue(tr3.compareTo(tr3) == 0);

    Assert.assertTrue(tr1.compareTo(new TabletRange(null, sp1)) == 0);
    Assert.assertTrue(tr2.compareTo(new TabletRange(sp1, sp2)) == 0);
    Assert.assertTrue(tr3.compareTo(new TabletRange(sp2, null)) == 0);

    Assert.assertTrue(new TabletRange(null, null).compareTo(new TabletRange(null, null)) == 0);
  }

  @Test
  public void testToRange() {
    for (String prev : new String[] {null, "foo"}) {
      for (String end : new String[] {null, "zoo"}) {
        Assert.assertEquals(new Range(prev, false, end, true), new TabletRange(prev == null ? null
            : Bytes.of(prev), end == null ? null : Bytes.of(end)).getRange());
      }
    }
  }
}
