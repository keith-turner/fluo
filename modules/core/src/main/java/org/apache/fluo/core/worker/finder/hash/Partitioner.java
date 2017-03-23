package org.apache.fluo.core.worker.finder.hash;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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
import org.apache.fluo.core.worker.TabletInfoCache;
import org.apache.fluo.core.worker.TabletInfoCache.TabletInfo;
import org.apache.hadoop.io.Text;

public class Partitioner {

  private CuratorFramework curator;
  private AtomicBoolean stopped = new AtomicBoolean(false);
  private PathChildrenCache childrenCache;
  private PersistentEphemeralNode myESNode;
  private int groupSize;
  private GroupInfo groupInfo;
  private ScheduledExecutorService schedExecutor;
  private TabletInfoCache<TabletData> tabletCache;

  private static final Text END = new Text("END");
  
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

          break;
        default:
          break;

      }
    }
  }

  private void hash(Hasher hasher, Text t) {
    if (t == null) {
      hasher.putBytes(new byte[0]);
    } else {
      hasher.putBytes(t.getBytes(), 0, t.getLength());
    }
  }

  private List<TabletInfo<?>> selectTablets(List<TabletInfo<?>> tablets, int groupId,
      int totalGroups) {
    List<TabletInfo<?>> tabletsForGroup = new ArrayList<>();
    for (TabletInfo<?> tabletInfo : tablets) {
      Hasher hasher = Hashing.murmur3_32().newHasher();
      hash(hasher, tabletInfo.getStart());
      hash(hasher, tabletInfo.getEnd());
      int hc = hasher.hash().asInt();

      if (Math.abs(hc) % totalGroups == groupId) {
        tabletsForGroup.add(tabletInfo);
      }
    }

    return tabletsForGroup;
  }

  static class GroupInfo {
    final int myGroupId;
    final int myId; //id within group... TODO rename
    final int totalGroups;
    final int myGroupSize;
    final String myData;
    final Set<String> otherData;

    GroupInfo(int myId, int myGroupId, int myGroupSize, int totalGroups, String myData,
        Set<String> otherData) {
      this.myId = myId;
      this.myGroupId = myGroupId;
      this.myGroupSize = myGroupSize;
      this.totalGroups = totalGroups;
      this.myData = myData;
      this.otherData = otherData;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof GroupInfo) {
        GroupInfo other = (GroupInfo) o;
        return other.myGroupId == myGroupId && other.myId == myId
            && other.totalGroups == totalGroups && other.myGroupSize == myGroupSize
            && other.otherData.equals(otherData) && other.myData.equals(myData);
      }
      return false;
    }

    public boolean isGroupInAgreement() {
      return !myData.isEmpty() && otherData.size() == 1 && otherData.contains(myData);
    }
  }

  // TODO unit test
  static GroupInfo getGroupInfo(String me, SortedMap<String, String> children, int groupSize) {

    // TODO backfill holes inorder to keep groups more stable OR look into using Hashing.consistentHash

    int count = 0;
    int myGroupId = -1;
    int myId = -1;
    int totalGroups = children.size() / groupSize + (children.size() % groupSize > 0 ? 1 : 0);

    for (Entry<String, String> entry : children.entrySet()) {
      if (entry.getKey().equals(me)) {
        myGroupId = count / groupSize;
        myId = count % groupSize;
        break;
      }
      count++;
    }

    Set<String> otherData = new HashSet<>();
    String myData = "";
    count = 0;
    int myGroupSize = 0;

    for (Entry<String, String> entry : children.entrySet()) {
      if (count >= myGroupId * groupSize) {
        if(entry.getValue().equals("")) {
          throw new IllegalArgumentException();
        }
        if (entry.getKey().equals(me)) {
          myData = entry.getValue();
        } else {
          otherData.add(entry.getValue());
        }
        myGroupSize++;
      }

      count++;

      if (count == (myGroupId + 1) * groupSize + 1) {
        break;
      }
    }

    return new GroupInfo(myId, myGroupId, myGroupSize, totalGroups, myData, otherData);
  }
  
  static String getTabletData(int groupSize, GroupInfo gi, List<TabletInfo<?>> tablets) {
    
    List<Text> groupEndRows = tablets.stream().map(TabletInfo::getEnd).sorted((e1, e2) -> {
      if (e1 != null && e2 != null) {
        return e1.compareTo(e2);
      }
      if (e1 == null) {
        if (e2 == null) {
          return 0;
        }
        return 1;
      }

      return 1;
    }).map(e -> e == null ? END : e)
        .filter(
            e -> Math.abs(Hashing.murmur3_32().hashBytes(e.getBytes(), 0, e.getLength()).asInt())
                % gi.myGroupSize == gi.myId)
        .collect(Collectors.toList());

    Hasher hasher = Hashing.murmur3_32().newHasher();
    for (Text er : groupEndRows) {
      hasher.putBytes(er.getBytes(), 0, er.getLength());
    }
    
    String hash = hasher.hash().toString();
    
    return String.format("%d:%d:%s", groupSize, groupEndRows.size(), hash);
  }
  
  private synchronized void updateFinders() {
    // TODO put groupSize in zookeeper data?

    String me = myESNode.getActualPath();
    while (me == null) {
      UtilWaitThread.sleep(100);
      me = myESNode.getActualPath();
    }
    me = ZKPaths.getNodeFromPath(me);


    SortedMap<String, String> children = new TreeMap<>();
    for (ChildData childData : childrenCache.getCurrentData()) {
      children.put(ZKPaths.getNodeFromPath(childData.getPath()),
          new String(childData.getData(), StandardCharsets.UTF_8));
    }

    GroupInfo newGroupInfo = getGroupInfo(me, children, groupSize);
    if(!newGroupInfo.equals(groupInfo)) {
      if(!newGroupInfo.isGroupInAgreement()) {
        List<TabletInfo<?>> tablets = tabletCache.getTablets(true);
        String newData = getTabletData(groupSize, newGroupInfo, tablets);
        
        if(!newGroupInfo.myData.equals(newData)) {
          try {
            myESNode.setData(newData.getBytes(StandardCharsets.UTF_8));
          } catch (RuntimeException e) {
            throw e;
          } catch (Exception e) {
            //TODO let this fall through to rerun later?  and just log?
            throw new RuntimeException(e);
          }
        }
       
        groupInfo = null;
        schedExecutor.schedule(()-> updateFinders(), 500, TimeUnit.SECONDS);
        //TODO schedule a task to call this
      } else {
        groupInfo = newGroupInfo;
      }
    }
    
  }

  Partitioner(CuratorFramework curator) {
    this.curator = curator;
    try {
      myESNode = new PersistentEphemeralNode(curator, Mode.EPHEMERAL_SEQUENTIAL,
          ZookeeperPath.FINDERS + "/f-", new byte[0]);
      myESNode.start();
      myESNode.waitForInitialCreate(1, TimeUnit.MINUTES);

      childrenCache = new PathChildrenCache(curator, ZookeeperPath.FINDERS, true);
      childrenCache.getListenable().addListener(new FindersListener());
      childrenCache.start(StartMode.BUILD_INITIAL_CACHE);

      updateFinders();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  synchronized GroupInfo getCurrentGroup() {
    return groupInfo;
  }
}
