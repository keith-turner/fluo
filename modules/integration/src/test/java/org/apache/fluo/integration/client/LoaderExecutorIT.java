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

package org.apache.fluo.integration.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.fluo.api.client.Loader;
import org.apache.fluo.api.client.LoaderExecutor;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.exceptions.AlreadySetException;
import org.apache.fluo.integration.ITBaseMini;
import org.junit.Assert;
import org.junit.Test;

public class LoaderExecutorIT extends ITBaseMini {

  public static class BadLoader implements Loader {

    @Override
    public void load(TransactionBase tx, Context context) throws Exception {
      tx.set("r", new Column("f", "q"), "v");
      // setting same thing should cause exception
      tx.set("r", new Column("f", "q"), "v2");
    }

  }

  @Test
  public void testLoaderFailure() {
    LoaderExecutor le = client.newLoaderExecutor();
    le.execute(new BadLoader());

    try {
      le.close();
      Assert.fail();
    } catch (RuntimeException e) {
      Assert.assertEquals(AlreadySetException.class, e.getCause().getClass());
    }
  }

  private static void increment(TransactionBase tx, Loader.Context context) {
    int c = Integer.parseInt(tx.gets("datum90", new Column("info", "count"), "0")) + 1;
    tx.set("datum90", new Column("info", "count"), c + "");
  }

  @Test
  public void testSubmit() {
    try (LoaderExecutor le = client.newLoaderExecutor()) {

      List<CompletableFuture<?>> futures = new ArrayList<>();

      for (int i = 0; i < 10; i++) {
        futures.add(le.submit(LoaderExecutorIT::increment));
      }

      futures.forEach(f -> f.join());

      try (Snapshot snap = client.newSnapshot()) {
        Assert.assertEquals("10", snap.gets("datum90", new Column("info", "count")));
      }

      futures.clear();

      for (int i = 0; i < 10; i++) {
        futures.add(le.submit(LoaderExecutorIT::increment));
      }

      futures.forEach(f -> f.join());

      try (Snapshot snap = client.newSnapshot()) {
        Assert.assertEquals("20", snap.gets("datum90", new Column("info", "count")));
      }
    }
  }
}
