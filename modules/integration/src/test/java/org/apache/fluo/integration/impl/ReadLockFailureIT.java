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

import java.util.Map.Entry;
import java.util.function.Consumer;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.accumulo.format.FluoFormatter;
import org.apache.fluo.api.client.Transaction;
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

  // TODO test name
  @Test
  public void test1() throws Exception {

    try (Transaction tx = client.newTransaction()) {
      setAlias(tx, "node1", "bob");
      setAlias(tx, "node2", "joe");
      setAlias(tx, "node3", "alice");
      tx.commit();
    }

    TransactorNode t2 = new TransactorNode(env);
    TestTransaction tx2 = new TestTransaction(env, t2);

    addEdge(tx2, "node1", "node3");

    CommitData cd = tx2.createCommitData();
    Assert.assertTrue(tx2.preCommit(cd));

    t2.close();


    try (Transaction tx = client.newTransaction()) {
      setAlias(tx, "node1", "bobby");
      try {
        tx.commit();
      } catch (CommitException ce) {
        ce.printStackTrace();
      }
    }

    dumpTable(System.out::println);
  }
}
