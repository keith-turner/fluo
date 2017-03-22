package org.apache.fluo.core.worker.finder.hash;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode;
import org.apache.curator.utils.ZKPaths;
import org.apache.fluo.accumulo.util.ZookeeperPath;
import org.apache.fluo.core.util.UtilWaitThread;
import org.apache.fluo.core.worker.TabletInfoCache.TabletInfo;
import org.apache.fluo.core.worker.finder.hash.HashNotificationFinder.FindersListener;
import org.apache.hadoop.io.Text;

public class Coordinator {
  
  private CuratorFramework curator;
  private AtomicBoolean stopped = new AtomicBoolean(false);
  private PathChildrenCache childrenCache;
  private PersistentEphemeralNode myESNode;
  private int groupSize;
  
  private class FindersListener implements PathChildrenCacheListener {

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
      switch (event.getType()) {
        case CHILD_ADDED:
        case CHILD_REMOVED:
          if (!stopped.get()) {
            updateFinders();
          }
          break;
        case CHILD_UPDATED:
          log.warn("unexpected event " + event);
          break;
        default:
          break;

      }
    }
  }
  
  private void hash(Hasher hasher, Text t){
    if(t == null) {
      hasher.putBytes(new byte[0]);
    } else {
      hasher.putBytes(t.getBytes(), 0, t.getLength());
    }
  }
  
  private List<TabletInfo<?>> selectTablets(List<TabletInfo<?>> tablets, int groupId, int totalGroups){
    List<TabletInfo<?>> tabletsForGroup = new ArrayList<>();
    for (TabletInfo<?> tabletInfo : tablets) {
      Hasher hasher = Hashing.murmur3_32().newHasher();
      hash(hasher, tabletInfo.getStart());
      hash(hasher, tabletInfo.getEnd());
      int hc = hasher.hash().asInt();
      
      if(Math.abs(hc) % totalGroups == groupId) {
        tabletsForGroup.add(tabletInfo);
      }
    }
    
    return tabletsForGroup;
  }
  
  private synchronized void updateFinders() {

    String me = myESNode.getActualPath();
    while (me == null) {
      UtilWaitThread.sleep(100);
      me = myESNode.getActualPath();
    }
    me = ZKPaths.getNodeFromPath(me);

    SortedMap<String, String> children = new TreeMap<>();
    for (ChildData childData : childrenCache.getCurrentData()) {
      children.put(ZKPaths.getNodeFromPath(childData.getPath()), new String(childData.getData(), StandardCharsets.UTF_8));
    }
    
    List<TabletInfo<?>> tablets;
    
    //TODO make the following group code unit testable
    int count = 0;
    int myGroupId = -1;
    int myId = -1;
    int totalGroups = children.size() / groupSize + (children.size() % groupSize > 0 ? 1 : 0);
    
    for (Entry<String,String> entry : children.entrySet()) {
      if(entry.getKey().equals(me)) {
        myGroupId = count / groupSize;
        myId = count % groupSize;
        break;
      }
      count++;
    }
    
    
    SortedMap<String, String> group = new TreeMap<>();
    count = 0;
    
    for (Entry<String,String> entry : children.entrySet()) {
      if(count >= myGroupId*groupSize) {
        group.put(entry.getKey(), entry.getValue());
      }
      
      count++;
      
      if(count == (myGroupId+1) * groupSize+1){
        break;
      }
    }
    
    int myGroupSize = group.size();
    
    
    

    if (!finders.equals(children)) {
      int index = children.indexOf(me);
      if (index == -1) {
        this.modParams = null;
        finders = Collections.emptyList();
        log.debug("Did not find self in list of finders " + me);
      } else {
        updates++;
        this.modParams = new ModulusParams(children.indexOf(me), children.size(), updates);
        finders = children;
        log.debug("updated modulus params " + modParams.remainder + " " + modParams.divisor);
      }
    }
  }
  
  Coordinator(CuratorFramework curator) {
    this.curator = curator;
    try {
      myESNode =
          new PersistentEphemeralNode(curator, Mode.EPHEMERAL_SEQUENTIAL, ZookeeperPath.FINDERS
              + "/f-", new byte[0]);
      myESNode.start();
      myESNode.waitForInitialCreate(1, TimeUnit.MINUTES);

      childrenCache =
          new PathChildrenCache(curator, ZookeeperPath.FINDERS, true);
      childrenCache.getListenable().addListener(new FindersListener());
      childrenCache.start(StartMode.BUILD_INITIAL_CACHE);

      updateFinders();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
}
