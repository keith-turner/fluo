package org.apache.fluo.integration.impl;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.accumulo.format.FluoFormatter;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.exceptions.CommitException;
import org.apache.fluo.integration.ITBaseImpl;
import org.apache.fluo.integration.TestTransaction;
import org.junit.Test;

public class ReadLockIT extends ITBaseImpl {

  private void addEdge(TransactionBase tx, String node1, String node2) {
    String alias1 = tx.withReadLock().gets(node1, new Column("node","alias"));
    String alias2 = tx.withReadLock().gets(node2, new Column("node","alias"));

    tx.set(alias1+":"+alias2, new Column("edge",node1+":"+node2), "");
    tx.set(alias2+":"+alias1, new Column("edge",node2+":"+node1), "");
  }

  @Test
  public void testConcurrentReadlocks() throws Exception {


    TestTransaction tx0 = new TestTransaction(env);

    tx0.set("node1", new Column("node","alias"), "bob");
    tx0.set("node2", new Column("node","alias"), "joe");
    tx0.set("node3", new Column("node","alias"), "alice");

    tx0.commit();
    tx0.close();


    TestTransaction tx1 = new TestTransaction(env);
    tx1.set("node2", new Column("node","alias"), "jojo");


    TestTransaction tx2 = new TestTransaction(env);
    TestTransaction tx3 = new TestTransaction(env);

    addEdge(tx2, "node1","node2");
    addEdge(tx3, "node1","node3");

    tx2.commit();
    tx2.close();

    tx3.commit();
    tx3.close();

    //TODO verify data

    try {
      tx1.commit();
    } catch (CommitException ce) {
      ce.printStackTrace();
    }

    printSnapshot();

    Scanner scanner = conn.createScanner(getCurTableName(), Authorizations.EMPTY);

    for (Entry<Key, Value> entry : scanner) {
      System.out.println(FluoFormatter.toString(entry));
    }


  }

  @Test
  public void testWriteCausesReadLockToFail() throws Exception {
    TestTransaction tx0 = new TestTransaction(env);

    tx0.set("node1", new Column("node","alias"), "bob");
    tx0.set("node2", new Column("node","alias"), "joe");

    tx0.commit();
    tx0.close();

    TestTransaction tx1 = new TestTransaction(env);
    tx1.set("node2", new Column("node","alias"), "jojo");

    TestTransaction tx2 = new TestTransaction(env);

    addEdge(tx2, "node1","node2");

    tx1.commit();
    tx1.close();


    //TODO verify data

    try {
      tx2.commit();
    } catch (CommitException ce) {
      ce.printStackTrace();
    }

    printSnapshot();

    Scanner scanner = conn.createScanner(getCurTableName(), Authorizations.EMPTY);

    for (Entry<Key, Value> entry : scanner) {
      System.out.println(FluoFormatter.toString(entry));
    }
  }
}
