package org.apache.fluo.core.worker.finder.hash;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode;
import org.apache.curator.utils.ZKPaths;
import org.apache.fluo.accumulo.util.ZookeeperPath;
import org.apache.fluo.core.impl.Notification;
import org.apache.fluo.core.util.FluoThreadFactory;
import org.apache.fluo.core.util.HashingCollector;
import org.apache.fluo.core.util.UtilWaitThread;
import org.apache.fluo.core.worker.NotificationProcessor;
import org.apache.fluo.core.worker.TabletInfoCache;
import org.apache.fluo.core.worker.TabletInfoCache.TabletInfo;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParitionManager {

  private static final Logger log = LoggerFactory.getLogger(ParitionManager.class);
  
  private PathChildrenCache childrenCache;
  private PersistentEphemeralNode myESNode;
  private int groupSize = 7; //TODO make config
  private PartitionInfo partitionInfo;
  private ScheduledExecutorService schedExecutor;
  private TabletInfoCache<TabletData> tabletCache;

  private ScheduledFuture<?> scheduledUpdate;

  private NotificationProcessor processor;
  
  
  private static final Text END = new Text("END");
  
  private class FindersListener implements PathChildrenCacheListener {

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
      switch (event.getType()) {
        case CHILD_ADDED:
        case CHILD_REMOVED:
        case CHILD_UPDATED:
          scheduleUpdate(0, TimeUnit.SECONDS, false);
          break;
        default:
          break;

      }
    }
  }

  static class PartitionInfo {
    final int groupId;
    final int idInGroup; //id within group... TODO rename
    final int groups;
    final int groupSize;
    final int workers;

    PartitionInfo(int myId, int myGroupId, int myGroupSize, int totalGroups, int totalWorkers) {
      this.idInGroup = myId;
      this.groupId = myGroupId;
      this.groupSize = myGroupSize;
      this.groups = totalGroups;
      this.workers = totalWorkers;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof PartitionInfo) {
        PartitionInfo other = (PartitionInfo) o;
        return other.groupId == groupId && other.idInGroup == idInGroup
            && other.groups == groups && other.groupSize == groupSize && other.workers == workers;
      }
      return false;
    }
    
    @Override
    public String toString(){
      return String.format("workers:%d  groups:%d  groupSize:%d  groupId:%d  idInGroup:%d", workers, groups, groupSize, groupId, idInGroup);
    }
  }

  // TODO unit test
  static PartitionInfo getGroupInfo(String me, SortedSet<String> children, int groupSize) {

    // TODO backfill holes inorder to keep groups more stable OR look into using Hashing.consistentHash

    int count = 0;
    int myGroupId = -1;
    int myId = -1;
    int totalGroups = children.size() / groupSize + (children.size() % groupSize > 0 ? 1 : 0);

    for (String child : children) {
      if (child.equals(me)) {
        myGroupId = count / groupSize;
        myId = count % groupSize;
        break;
      }
      count++;
    }

    
    count = 0;
    int myGroupSize = 0;

    for (int i =0; i< children.size(); i++){
      if (count >= myGroupId * groupSize) {
        myGroupSize++;
      }

      count++;

      if (count == (myGroupId + 1) * groupSize + 1) {
        break;
      }
    }

    return new PartitionInfo(myId, myGroupId, myGroupSize, totalGroups, children.size());
  }
  
  //TODO better method name or inline?
  static String hashTablets(int groupSize, List<TabletInfo<?>> tablets) {
    
    String hash = tablets.stream() // create stream of tablet info
        .map(TabletInfo::getEnd) //map to the tablets end row
        .map(er -> er == null ? END : er) //map null to something... this may not sort properly, but that does not matter
        .sorted(Text::compareTo) //put in a consistent order for hashing
        .collect(HashingCollector.murmur3_32(TabletPartitioner::hash)) //hash all the text objects
        .toString();

    return String.format("%d:%s", groupSize, hash);
  }
   
  private void updatePartitionInfo(boolean canSetPartitionInfo) {
    try{
      String me = myESNode.getActualPath();
      while (me == null) {
        UtilWaitThread.sleep(100);
        me = myESNode.getActualPath();
      }
      me = ZKPaths.getNodeFromPath(me);
  
      HashSet<String> uniqueData = new HashSet<>();
      
      uniqueData.add(hashTablets(groupSize, tabletCache.getTablets(false)));
      
      SortedSet<String> children = new TreeSet<>();
      for (ChildData childData : childrenCache.getCurrentData()) {
        String node = ZKPaths.getNodeFromPath(childData.getPath());
        String data = new String(childData.getData(), StandardCharsets.UTF_8);
        children.add(node);
        uniqueData.add(data);
      }
  
      if(!children.contains(me)) {
        log.warn("Did not see self ("+me+"), cannot gather tablet and notification partitioning info.");
        setPartitionInfo(null); //disable this worker from processing notifications
        scheduleUpdate(15, TimeUnit.SECONDS, false);
        return;
      }
      
      PartitionInfo currentPI = getPartitionInfo();
      PartitionInfo newPI = getGroupInfo(me, children, groupSize);
      
      if(uniqueData.size() != 1 || currentPI == null) {
        //not all workers are in agreement on the set of split points in the accumulo table OR we just started
        setPartitionInfo(null); //set this to null a quickly as possible to halt any current work... TODO also need to clear queued notifications
        List<TabletInfo<?>> tablets = tabletCache.getTablets(true);
        String hash = hashTablets(groupSize, tablets);
        
        myESNode.setData(hash.getBytes(StandardCharsets.UTF_8));
        
        //want to avoid every worker recomputing every time every other worker updates
        scheduleUpdate(15, TimeUnit.SECONDS, true); //TODO time
      } else if(!newPI.equals(currentPI)) {
        if(canSetPartitionInfo) {
          setPartitionInfo(newPI);
        } else {
          setPartitionInfo(null);  //TODO clear ntfy Q
          scheduleUpdate(15, TimeUnit.SECONDS, true);
        }      
      }
    }catch(Exception e) {
      log.warn("Problem gathering tablet and notification partitioning info.", e);
      setPartitionInfo(null); //disable this worker from processing notifications
      scheduleUpdate(15, TimeUnit.SECONDS, false);
    }
  }

  private synchronized void scheduleUpdate(long delay, TimeUnit tu, boolean canSetPartitionInfo) {
    if(scheduledUpdate == null || scheduledUpdate.isDone()) {
      scheduledUpdate = schedExecutor.schedule(() -> updatePartitionInfo(canSetPartitionInfo), delay, tu);
    }
  }
  
  private class CheckTabletsTask implements Runnable {
    @Override
    public void run() {
      try{
        HashSet<String> uniqueData = new HashSet<>();
      
        uniqueData.add(hashTablets(groupSize, tabletCache.getTablets(false)));
      
        for (ChildData childData : childrenCache.getCurrentData()) {
          String data = new String(childData.getData(), StandardCharsets.UTF_8);
          uniqueData.add(data);
        }
      
        if(uniqueData.size() > 1) {
          scheduleUpdate(0, TimeUnit.SECONDS, false);
        }
      }catch(Exception e){
        //TODO log
      }
    }
  }
  
  ParitionManager(CuratorFramework curator, NotificationProcessor processor) {
    try {
      this.processor = processor;
      
      myESNode = new PersistentEphemeralNode(curator, Mode.EPHEMERAL_SEQUENTIAL,
          ZookeeperPath.FINDERS + "/f-", new byte[0]);
      myESNode.start();
      myESNode.waitForInitialCreate(1, TimeUnit.MINUTES);

      childrenCache = new PathChildrenCache(curator, ZookeeperPath.FINDERS, true);
      childrenCache.getListenable().addListener(new FindersListener());
      childrenCache.start(StartMode.BUILD_INITIAL_CACHE);

      //TODO is there  shared thread pool that could be used??
      schedExecutor = Executors.newScheduledThreadPool(1, new FluoThreadFactory("Fluo worker partition manager"));
      schedExecutor.scheduleWithFixedDelay(new CheckTabletsTask(), 5, 5, TimeUnit.MINUTES); 
      scheduleUpdate(0, TimeUnit.SECONDS, true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  private void setPartitionInfo(PartitionInfo pi){
    synchronized (this) {
      this.partitionInfo = pi;
      this.notifyAll();  
    }
    log.debug("Set partition info : "+pi);
    processor.clear();
  }
  
  synchronized PartitionInfo waitForPartitionInfo() throws InterruptedException {
    while(partitionInfo == null) {
      wait(500);
    }
    
    return partitionInfo;
  }
  
  synchronized PartitionInfo getPartitionInfo() {
    return partitionInfo;
  }

  public void stop() {
    schedExecutor.shutdownNow();
    //TODO wait???
  }

  public boolean shouldProcess(Notification notification) {
    // TODO Auto-generated method stub
    return true;
  }
}
