package org.apache.fluo.accumulo.util;

import org.apache.accumulo.core.data.Key;

public class ReadLockUtil {
  private static final long DEL_MASK = 0x0000000000000001L;

  // TODO code was copied from NotifyUtil

  public static boolean isDelete(Key k) {
    return isDelete(k.getTimestamp());
  }

  public static boolean isDelete(long ts) {
    return (ts & DEL_MASK) == DEL_MASK;
  }

  public static long encodeTs(long ts, boolean isDelete) {
    // TODO check 1st three bits are 000
    return ts << 1 | (isDelete ? 1 : 0);
  }

  public static long decodeTs(Key k) {
    return decodeTs(k.getTimestamp());
  }

  public static long decodeTs(long ts) {
    return ts >> 1;
  }
}
