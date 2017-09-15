package org.apache.fluo.integration.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.accumulo.format.FluoFormatter;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Loader;
import org.apache.fluo.api.client.LoaderExecutor;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.api.exceptions.CommitException;
import org.apache.fluo.integration.ITBaseImpl;
import org.apache.fluo.integration.TestTransaction;
import org.junit.Assert;
import org.junit.Test;

import static java.util.Arrays.asList;

public class ReadLockIT extends ITBaseImpl {

  private static final Column ALIAS_COL = new Column("node", "alias");

  private void addEdge(String node1, String node2) {
    try (Transaction tx = client.newTransaction()) {
      addEdge(tx, node1, node2);
      tx.commit();
    }
  }

  private void addEdge(TransactionBase tx, String node1, String node2) {
    Map<String, Map<Column, String>> aliases =
        tx.withReadLock().gets(asList("r:" + node1, "r:" + node2), ALIAS_COL); // TODO need to test
                                                                               // all get
                                                                               // methods
    String alias1 = aliases.get("r:" + node1).get(ALIAS_COL);
    String alias2 = aliases.get("r:" + node1).get(ALIAS_COL);

    addEdge(tx, node1, node2, alias1, alias2);
  }

  private void addEdge(TransactionBase tx, String node1, String node2, String alias1,
      String alias2) {
    tx.set("d:" + alias1 + ":" + alias2, new Column("edge", node1 + ":" + node2), "");
    tx.set("d:" + alias2 + ":" + alias1, new Column("edge", node2 + ":" + node1), "");

    tx.set("r:" + node1 + ":" + node2, new Column("edge", "aliases"), alias1 + ":" + alias2);
    tx.set("r:" + node2 + ":" + node1, new Column("edge", "aliases"), alias2 + ":" + alias1);
  }

  private void setAlias(TransactionBase tx, String node, String alias) {
    tx.set("r:" + node, new Column("node", "alias"), alias);

    CellScanner scanner = tx.scanner().over(Span.prefix("r:" + node + ":")).build();

    for (RowColumnValue rcv : scanner) {
      String otherNode = rcv.getsRow().split(":")[2];
      String[] aliases = rcv.getsValue().split(":");

      if (aliases.length != 2) {
        System.out.println(rcv);
      }

      if (!alias.equals(aliases[0])) {
        tx.delete("d:" + aliases[0] + ":" + aliases[1], new Column("edge", node + ":" + otherNode));
        tx.delete("d:" + aliases[1] + ":" + aliases[0], new Column("edge", otherNode + ":" + node));

        addEdge(tx, node, otherNode, alias, aliases[1]);
      }
    }
  }

  @Test
  public void testConcurrentReadlocks() throws Exception {

    try (Transaction tx = client.newTransaction()) {
      setAlias(tx, "node1", "bob");
      setAlias(tx, "node2", "joe");
      tx.commit();
    }


    TestTransaction tx1 = new TestTransaction(env);
    setAlias(tx1, "node2", "jojo");

    TestTransaction tx2 = new TestTransaction(env);
    TestTransaction tx3 = new TestTransaction(env);

    addEdge(tx2, "node1", "node2");
    addEdge(tx3, "node1", "node3");

    tx2.commit();
    tx2.close();

    tx3.commit();
    tx3.close();

    // TODO verify data

    try {
      tx1.commit();
      Assert.fail("Expected exception");
    } catch (CommitException ce) {
      // ce.printStackTrace();
    }
  }

  @Test
  public void testWriteCausesReadLockToFail() throws Exception {
    try (Transaction tx = client.newTransaction()) {
      setAlias(tx, "node1", "bob");
      setAlias(tx, "node2", "joe");
      tx.commit();
    }

    TestTransaction tx1 = new TestTransaction(env);
    setAlias(tx1, "node2", "jojo");

    TestTransaction tx2 = new TestTransaction(env);

    addEdge(tx2, "node1", "node2");

    tx1.commit();
    tx1.close();


    // TODO verify data

    try {
      tx2.commit();
      Assert.fail("Expected exception");
    } catch (CommitException ce) {
      // ce.printStackTrace();
    }
  }

  private void dumpTable() throws TableNotFoundException {
    Scanner scanner = conn.createScanner(getCurTableName(), Authorizations.EMPTY);

    for (Entry<Key, Value> entry : scanner) {
      System.out.println(FluoFormatter.toString(entry));
    }
  }

  @Test
  public void testWriteAfterReadLock() throws Exception {
    try (Transaction tx = client.newTransaction()) {
      setAlias(tx, "node1", "bob");
      setAlias(tx, "node2", "joe");
      setAlias(tx, "node3", "alice");

      tx.commit();
    }

    addEdge("node1", "node2");

    try (Transaction tx = client.newTransaction()) {
      setAlias(tx, "node1", "bobby");
      tx.commit();
    }

    addEdge("node1", "node3");
    // TODO verify data


    printSnapshot();
    dumpTable();
  }

  @Test
  public void testRandom() throws Exception {
    int numAliases = 75; // TODO
    int numNodes = 100;
    int numEdges = 1000;
    int numAliasChanges = 25;

    Random rand = new Random();

    Map<String, String> nodes = new HashMap<>();
    while (nodes.size() < numNodes) {
      // TODO have some nodes with same alias
      nodes.put(String.format("n-%09d", rand.nextInt(1000000000)),
          String.format("a-%09d", rand.nextInt(1000000000)));
    }

    List<String> nodesList = new ArrayList<>(nodes.keySet());
    Set<String> edges = new HashSet<>();
    while (edges.size() < numEdges) {
      String n1 = nodesList.get(rand.nextInt(nodesList.size()));
      String n2 = nodesList.get(rand.nextInt(nodesList.size()));
      if (n1.equals(n2) || edges.contains(n2 + ":" + n1)) {
        continue;
      }

      edges.add(n1 + ":" + n2);
    }

    try (LoaderExecutor le = client.newLoaderExecutor()) {
      for (Entry<String, String> entry : nodes.entrySet()) {
        le.execute((tx, ctx) -> setAlias(tx, entry.getKey(), entry.getValue()));
      }
    }

    List<Loader> loadOps = new ArrayList<>();
    for (String edge : edges) {
      String[] enodes = edge.split(":");
      loadOps.add((tx, ctx) -> addEdge(tx, enodes[0], enodes[1]));
    }

    Map<String, String> nodes2 = new HashMap<>(nodes);
    for (int i = 0; i < numAliasChanges; i++) {
      String node = nodesList.get(rand.nextInt(nodesList.size()));
      String alias = String.format("a-%09d", rand.nextInt(1000000000));
      loadOps.add((tx, ctx) -> setAlias(tx, node, alias));
      nodes2.put(node, alias);
    }

    Collections.shuffle(loadOps, rand);

    FluoConfiguration conf = new FluoConfiguration(config);
    conf.setLoaderThreads(20);
    try (FluoClient client = FluoFactory.newClient(conf);
        LoaderExecutor le = client.newLoaderExecutor()) {
      loadOps.forEach(loader -> le.execute(loader));
    }

    Set<String> expectedEdges = new HashSet<>();
    for (String edge : edges) {
      String[] enodes = edge.split(":");
      String alias1 = nodes2.get(enodes[0]);
      String alias2 = nodes2.get(enodes[1]);

      expectedEdges.add(alias1 + ":" + alias2);
      expectedEdges.add(alias2 + ":" + alias1);
    }

    Set<String> actualEdges = new HashSet<>();
    try (Snapshot snap = client.newSnapshot()) {
      snap.scanner().over(Span.prefix("d:")).build().stream().map(RowColumnValue::getsRow)
          .map(r -> r.substring(2)).forEach(actualEdges::add);
    }

    Assert.assertEquals(expectedEdges, expectedEdges);
  }
}
