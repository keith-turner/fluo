package org.apache.fluo.core.worker.finder.hash;

import java.util.Objects;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.fluo.api.data.Bytes;

public class Tablet {
  private final Bytes prevEndRow;
  private final Bytes endRow;
  private final int hc;
  
  public Tablet(Bytes per, Bytes er) {
    this.prevEndRow = per;
    this.endRow = er;
    
    Hasher hasher = Hashing.murmur3_32().newHasher();
    if(per != null){
      hasher.putBytes(per.toArray());
    }
    
    if(er != null){
      hasher.putBytes(er.toArray());
    }
    
    this.hc = hasher.hash().asInt();
  }
  
  @Override
  public int hashCode(){
    return hc;
  }
  
  public int persistentHashCode(){
    return hc;
  }
  
  @Override
  public boolean equals(Object o) {
    if(o instanceof Tablet){
      Tablet ot = (Tablet)o;
      return Objects.equals(prevEndRow, ot.prevEndRow) && Objects.equals(endRow, ot.endRow);  
    }
    
    return false;
  }

  public Bytes getPrevEndRow() {
    return prevEndRow;
  }
  
  public Bytes getEndRow() {
    return endRow;
  }

  public boolean contains(Bytes row) {
    return (prevEndRow == null || row.compareTo(prevEndRow) > 0) && (endRow == null || row.compareTo(endRow) <= 0);
  }
  
  @Override
  public String toString(){
    return getPrevEndRow()+" "+getEndRow();
    
  }
}
