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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
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
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.api.exceptions.CommitException;
import org.apache.fluo.core.impl.TransactionImpl.CommitData;
import org.apache.fluo.core.oracle.Stamp;
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

  private void expectCommitException(Consumer<Transaction> retryAction) {
    try (Transaction tx = client.newTransaction()) {
      retryAction.accept(tx);
      tx.commit();
      Assert.fail();
    } catch (CommitException ce) {

    }
  }

  private void retryOnce(Consumer<Transaction> retryAction) {

    expectCommitException(retryAction);

    try (Transaction tx = client.newTransaction()) {
      retryAction.accept(tx);
      tx.commit();
    }
  }

  private void retryTwice(Consumer<Transaction> retryAction) {

    expectCommitException(retryAction);
    expectCommitException(retryAction);

    try (Transaction tx = client.newTransaction()) {
      retryAction.accept(tx);
      tx.commit();
    }
  }


  private void partiallyCommit(Consumer<TransactionBase> action, boolean commitPrimary)
      throws Exception {
    TransactorNode t2 = new TransactorNode(env);
    TestTransaction tx2 = new TestTransaction(env, t2);

    action.accept(tx2);

    CommitData cd = tx2.createCommitData();
    Assert.assertTrue(tx2.preCommit(cd));

    if (commitPrimary) {
      Stamp commitTs = env.getSharedResources().getOracleClient().getStamp();
      Assert.assertTrue(tx2.commitPrimaryColumn(cd, commitTs));
    }

    t2.close();
  }

  // TODO test name
  @Test
  public void testBasicRollback() throws Exception {

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

    partiallyCommit(tx -> addEdge(tx, "node1", "node3"), false);

    Assert.assertEquals(ImmutableSet.of("bob:joe", "joe:bob"), getDerivedEdges());

    retryOnce(tx -> setAlias(tx, "node1", "bobby"));

    Assert.assertEquals(ImmutableSet.of("bobby:joe", "joe:bobby"), getDerivedEdges());

    retryOnce(tx -> setAlias(tx, "node3", "alex"));

    Assert.assertEquals(ImmutableSet.of("bobby:joe", "joe:bobby"), getDerivedEdges());
  }

  @Test
  public void testBasicRollforward() throws Exception {
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

    partiallyCommit(tx -> addEdge(tx, "node1", "node3"), true);

    retryOnce(tx -> setAlias(tx, "node1", "bobby"));

    Assert.assertEquals(ImmutableSet.of("bobby:joe", "joe:bobby", "bobby:alice", "alice:bobby"),
        getDerivedEdges());

    retryOnce(tx -> setAlias(tx, "node3", "alex"));

    Assert.assertEquals(ImmutableSet.of("bobby:joe", "joe:bobby", "bobby:alex", "alex:bobby"),
        getDerivedEdges());
  }


  @Test
  public void testParallelScan() throws Exception {

    Column crCol = new Column("stat", "completionRatio");

    try (Transaction tx = client.newTransaction()) {
      tx.set("user5", crCol, "0.5");
      tx.set("user6", crCol, "0.75");
      tx.commit();
    }

    partiallyCommit(tx -> {
      // get multiple read locks with a parallel scan
        Map<String, Map<Column, String>> ratios =
            tx.withReadLock().gets(Arrays.asList("user5", "user6"), crCol);

        double cr1 = Double.parseDouble(ratios.get("user5").get(crCol));
        double cr2 = Double.parseDouble(ratios.get("user5").get(crCol));

        tx.set("org1", crCol, (cr1 + cr2) / 2 + "");
      }, false);

    retryTwice(tx -> {
      Map<String, Map<Column, String>> ratios = tx.gets(Arrays.asList("user5", "user6"), crCol);

      tx.set("user5", crCol, "0.51");
      tx.set("user6", crCol, "0.76");
    });

    try (Snapshot snap = client.newSnapshot()) {
      Assert.assertNull(snap.gets("org1", crCol));
      Assert.assertEquals("0.51", snap.gets("user5", crCol));
      Assert.assertEquals("0.76", snap.gets("user6", crCol));
    }
  }

  @Test
  public void testParallelScan2() throws Exception {

    Column crCol = new Column("stat", "completionRatio");

    try (Transaction tx = client.newTransaction()) {
      tx.set("user5", crCol, "0.5");
      tx.set("user6", crCol, "0.75");
      tx.commit();
    }

    partiallyCommit(tx -> {
      // get multiple read locks with a parallel scan
        Map<RowColumn, String> ratios =
            tx.withReadLock().gets(
                Arrays.asList(new RowColumn("user5", crCol), new RowColumn("user6", crCol)));


        double cr1 = Double.parseDouble(ratios.get(new RowColumn("user5", crCol)));
        double cr2 = Double.parseDouble(ratios.get(new RowColumn("user6", crCol)));

        tx.set("org1", crCol, (cr1 + cr2) / 2 + "");
      }, false);

    retryTwice(tx -> {
      Map<RowColumn, String> ratios =
          tx.gets(Arrays.asList(new RowColumn("user5", crCol), new RowColumn("user6", crCol)));

      tx.set("user5", crCol, "0.51");
      tx.set("user6", crCol, "0.76");
    });

    try (Snapshot snap = client.newSnapshot()) {
      Assert.assertNull(snap.gets("org1", crCol));
      Assert.assertEquals("0.51", snap.gets("user5", crCol));
      Assert.assertEquals("0.76", snap.gets("user6", crCol));
    }
  }

  // TODO test time out rollback
}
