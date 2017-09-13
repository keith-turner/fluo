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
