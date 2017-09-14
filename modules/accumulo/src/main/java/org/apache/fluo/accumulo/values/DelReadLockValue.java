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

package org.apache.fluo.accumulo.values;

import org.apache.fluo.accumulo.util.ByteArrayUtil;

public class DelReadLockValue {

  // TODO maybe rollback does not need a commit ts
  public static byte[] encode(long commitTs, boolean isRollback) {
    byte[] ba = new byte[9];
    ba[0] = (byte) (isRollback ? 1 : 0);
    ByteArrayUtil.encode(ba, 1, commitTs);
    return ba;
  }

  public static long getCommitTimestamp(byte[] data) {
    return ByteArrayUtil.decodeLong(data, 1);
  }
}
