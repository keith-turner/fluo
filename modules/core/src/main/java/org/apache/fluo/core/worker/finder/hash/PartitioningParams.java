package org.apache.fluo.core.worker.finder.hash;

/**
 * These are the parameters used to partition tablets and notifications among workers.
 */
public class PartitioningParams {
  public int getTotalGroups(){
    return 0;
  }
  
  public int getMyGroupId() {
    return 0;
  }
  
  public int getGroupSize(){
    return 0;
  }
  
  public int getMyId(){
    return 0;
  }
  
  public String getGroupData() {
    return null;
  }
}
