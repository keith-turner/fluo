package org.apache.fluo.core.worker.finder.hash;

import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.fluo.api.data.Bytes;

public class TabletSet {
  private TreeMap<Bytes, Tablet> tmap;
  private Tablet lastTablet;
  
  public TabletSet(List<Tablet> tablets) {
    tmap = new TreeMap<>();
    
    for (Tablet tablet : tablets) {
      if(tablet.getEndRow() == null) {
        lastTablet = tablet;
      } else {
        tmap.put(tablet.getEndRow(), tablet);
      }
    }
  }
  
  public Tablet getContaining(Bytes row) {
    Entry<Bytes,Tablet> entry = tmap.ceilingEntry(row);
    if(entry != null) {
      if(entry.getValue().contains(row)) {
        return entry.getValue();
      }
    } else if(lastTablet != null) {
      if(lastTablet.contains(row)){
        return lastTablet;
      }
    }
    
    return null;
  }
}
