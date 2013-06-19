/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.accismus;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

/**
 * A service that looks for updated columns to process
 */
public class Worker {
  private ColumnSet observedColumns;
  private String table;
  private Connector conn;
  private Map<Column,Observer> colObservers;
  
  public Worker(String table, Connector connector, Map<Column,Observer> colObservers) {
    this.table = table;
    this.conn = connector;
    
    this.observedColumns = new ColumnSet();
    
    for (Column col : colObservers.keySet()) {
      observedColumns.add(col);
    }
    
    this.colObservers = colObservers;
    
  }
  
  public void processUpdates() throws Exception {
    Scanner scanner = conn.createScanner(table, new Authorizations());
    scanner.fetchColumnFamily(new Text(Constants.NOTIFY_CF));
    
    for (Entry<Key,Value> entry : scanner) {
      String ca[] = entry.getKey().getColumnQualifier().toString().split("\\|\\|", 2);
      Column col = new Column(ca[0], ca[1]);
      
      Observer observer = colObservers.get(col);
      if (observer == null) {
        // TODO do something
      }
      
      String row = entry.getKey().getRowData().toString();
      
      Transaction tx = new Transaction(table, conn, row, col, observedColumns);
      
      try {
        // TODO check ack to see if observer should run
        observer.process(tx, row, col);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
