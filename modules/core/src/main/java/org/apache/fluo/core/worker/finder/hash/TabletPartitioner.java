package org.apache.fluo.core.worker.finder.hash;

import java.util.function.Predicate;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.fluo.core.worker.TabletInfoCache.TabletInfo;
import org.apache.fluo.core.worker.finder.hash.ParitionManager.PartitionInfo;
import org.apache.hadoop.io.Text;

public class TabletPartitioner implements Predicate<TabletInfo<?>> {

  private PartitionInfo partition;
  
  TabletPartitioner(PartitionInfo pi){
    this.partition = pi;
  }
  
  static void hash(Hasher hasher, Text t) {
    if (t == null) {
      hasher.putBytes(new byte[0]);
    } else {
      hasher.putBytes(t.getBytes(), 0, t.getLength());
    }
  }
  
  @Override
  public boolean test(TabletInfo<?> t) {    
    Hasher hasher = Hashing.murmur3_32().newHasher();
    
    hash(hasher, t.getStart());
    hash(hasher, t.getEnd());
    
    int hc = Math.abs(hasher.hashCode());
    
    // TODO what about group size of zero?
    return hc % partition.groupSize == partition.idInGroup;
  }

}
