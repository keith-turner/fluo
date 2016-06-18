package org.apache.fluo.integration.impl;

import java.net.URL;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.Observer;
import org.apache.fluo.core.util.FluoClassloader;
import org.apache.fluo.integration.ITBaseMini;
import org.junit.Test;

public class FclIT extends ITBaseMini{
  @Test
  public void test1() throws Exception {
    ClassLoader fcl =
        new FluoClassloader(new URL[]{new URL("file:///.../phrasecount/target/phrasecount-0.0.1-SNAPSHOT.jar")});

    Observer observer = (Observer) fcl.loadClass("phrasecount.SimpleObserver").newInstance();

    try(FluoClient fc = FluoFactory.newClient(config)){
      try(Transaction tx = fc.newTransaction()){
        observer.process(tx, Bytes.of("h"), new Column("f","q"));
        tx.commit();
      }

      try(Snapshot snap = fc.newSnapshot()){
        System.out.println(snap.gets("r1", new Column("f","q")));
      }
    }
  }
}
