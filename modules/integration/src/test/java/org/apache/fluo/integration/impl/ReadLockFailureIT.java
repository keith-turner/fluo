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

package org.apache.fluo.integration.impl;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableSet;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.accumulo.format.FluoFormatter;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.api.exceptions.CommitException;
import org.apache.fluo.core.impl.TransactionImpl.CommitData;
import org.apache.fluo.core.impl.TransactorNode;
import org.apache.fluo.integration.ITBaseImpl;
import org.apache.fluo.integration.TestTransaction;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.fluo.integration.impl.ReadLockIT.addEdge;
import static org.apache.fluo.integration.impl.ReadLockIT.setAlias;

public class ReadLockFailureIT extends ITBaseImpl {

  private void dumpTable(Consumer<String> out) throws TableNotFoundException {
    Scanner scanner = conn.createScanner(getCurTableName(), Authorizations.EMPTY);
    for (Entry<Key, Value> entry : scanner) {
      out.accept(FluoFormatter.toString(entry));
    }
  }

  private Set<String> getDerivedEdges() {
    Set<String> derivedEdges = new HashSet<>();
    try (Snapshot snap = client.newSnapshot()) {
      snap.scanner().over(Span.prefix("d:")).build().stream().map(RowColumnValue::getsRow)
          .map(r -> r.substring(2)).forEach(derivedEdges::add);
    }
    return derivedEdges;
  }

  // TODO test that parallel scans get info on cols with locks

  // TODO test name
  @Test
  public void test1() throws Exception {

    try (Transaction tx = client.newTransaction()) {
      setAlias(tx, "node1", "bob");
      setAlias(tx, "node2", "joe");
      setAlias(tx, "node3", "alice");
      tx.commit();
    }

    try (Transaction tx = client.newTransaction()) {
      addEdge(tx, "node1", "node2");
      tx.commit();
    }

    TransactorNode t2 = new TransactorNode(env);
    TestTransaction tx2 = new TestTransaction(env, t2);

    addEdge(tx2, "node1", "node3");

    Assert.assertEquals(ImmutableSet.of("bob:joe", "joe:bob"), getDerivedEdges());

    CommitData cd = tx2.createCommitData();
    Assert.assertTrue(tx2.preCommit(cd));

    t2.close();

    try (Transaction tx = client.newTransaction()) {
      setAlias(tx, "node1", "bobby");
      try {
        tx.commit(); // commit should fail and roll back the read lock
      } catch (CommitException ce) {
        ce.printStackTrace();
      }
    }

    Assert.assertEquals(ImmutableSet.of("bob:joe", "joe:bob"), getDerivedEdges());

    // TODO automatically inspect raw data in accumulo??
    dumpTable(System.out::println);

    try (Transaction tx = client.newTransaction()) {
      setAlias(tx, "node1", "bobby");
      tx.commit();
    }

    Assert.assertEquals(ImmutableSet.of("bobby:joe", "joe:bobby"), getDerivedEdges());
  }
}
