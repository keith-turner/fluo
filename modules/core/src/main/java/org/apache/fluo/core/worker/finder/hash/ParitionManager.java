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

package org.apache.fluo.core.worker.finder.hash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode;
import org.apache.curator.utils.ZKPaths;
import org.apache.fluo.accumulo.iterators.NotificationHashFilter;
import org.apache.fluo.accumulo.util.NotificationUtil;
import org.apache.fluo.accumulo.util.ZookeeperPath;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.FluoConfigurationImpl;
import org.apache.fluo.core.impl.Notification;
import org.apache.fluo.core.util.ByteUtil;
import org.apache.fluo.core.util.FluoThreadFactory;
import org.apache.fluo.core.util.UtilWaitThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

public class ParitionManager {

  private static final Logger log = LoggerFactory.getLogger(ParitionManager.class);

  private final PathChildrenCache childrenCache;
  private final PersistentEphemeralNode myESNode;
  private final int groupSize;
  private long paritionSetTime;
  private PartitionInfo partitionInfo;
  private final ScheduledExecutorService schedExecutor;

  private CuratorFramework curator;

  private Environment env;

  private final long minSleepTime;
  private final long maxSleepTime;
  private long retrySleepTime;



  private static final long STABILIZE_TIME = TimeUnit.SECONDS.toNanos(60);

  private class FindersListener implements PathChildrenCacheListener {

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
      switch (event.getType()) {
        case CHILD_ADDED:
        case CHILD_REMOVED:
        case CHILD_UPDATED:
          scheduleUpdate();
          break;
        default:
          break;
      }
    }
  }

  static class PartitionInfo {
    final int groupId;
    final int idInGroup;
    final int groups;
    final int groupSize;
    final int workers;
    final TabletSet groupsTablets;

    PartitionInfo(int myId, int myGroupId, int myGroupSize, int totalGroups, int totalWorkers,
        List<TabletRange> groupsTablets) {
      this.idInGroup = myId;
      this.groupId = myGroupId;
      this.groupSize = myGroupSize;
      this.groups = totalGroups;
      this.workers = totalWorkers;
      this.groupsTablets = new TabletSet(groupsTablets);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof PartitionInfo) {
        PartitionInfo other = (PartitionInfo) o;
        return other.groupId == groupId && other.idInGroup == idInGroup && other.groups == groups
            && other.groupSize == groupSize && other.workers == workers
            && other.groupsTablets.equals(groupsTablets);
      }
      return false;
    }

    @Override
    public String toString() {
      return String.format(
          "workers:%d  groups:%d  groupSize:%d  groupId:%d  idInGroup:%d  #tablets:%d", workers,
          groups, groupSize, groupId, idInGroup, groupsTablets.size());
    }
  }

  // TODO unit test
  static PartitionInfo getGroupInfo(String me, SortedSet<String> children,
      Collection<TabletRange> tablets, int groupSize) {

    // TODO backfill holes inorder to keep groups more stable OR look into using
    // Hashing.consistentHash

    int count = 0;
    int myGroupId = -1;
    int myId = -1;

    for (String child : children) {
      if (child.equals(me)) {
        myGroupId = count / groupSize;
        myId = count % groupSize;
        break;
      }
      count++;
    }

    int groupStart = myGroupId * groupSize;
    int groupEnd = Math.min(children.size(), groupStart + groupSize);
    int myGroupSize = groupEnd - groupStart;
    
    int totalGroups = children.size() / groupSize + (children.size() % groupSize > 0 ? 1 : 0);

    int mgid = myGroupId;
    List<TabletRange> groupsTablets =
        tablets.stream().filter(tr -> Math.abs(tr.persistentHashCode()) % totalGroups == mgid)
            .collect(toList());

    return new PartitionInfo(myId, myGroupId, myGroupSize, totalGroups, children.size(),
        groupsTablets);
  }

  private void updatePartitionInfo() {
    try {
      String me = myESNode.getActualPath();
      while (me == null) {
        Thread.sleep(100);
        me = myESNode.getActualPath();
      }
      me = ZKPaths.getNodeFromPath(me);

      byte[] zkSplitData = null;
      SortedSet<String> children = new TreeSet<>();
      Set<String> groupSizes = new HashSet<>();
      for (ChildData childData : childrenCache.getCurrentData()) { // TODO ignore splits child
        String node = ZKPaths.getNodeFromPath(childData.getPath());
        if (node.equals("splits")) {
          zkSplitData = childData.getData();
        } else {
          children.add(node);
          groupSizes.add(new String(childData.getData(), UTF_8));
        }
      }

      // TODO log ONE info when not finding notifications ... and then log one info when finding
      // notifications....
      if (zkSplitData == null) {
        log.info("Did not find splits in zookeeper, will retry later.");
        setPartitionInfo(null); // disable this worker from processing notifications
        scheduleRetry();
        return;
      }

      if (!children.contains(me)) {
        log.warn("Did not see self (" + me
            + "), cannot gather tablet and notification partitioning info.");
        setPartitionInfo(null); // disable this worker from processing notifications
        scheduleRetry();
        return;
      }

      // ensure all workers agree on the group size
      if (groupSizes.size() != 1 || !groupSizes.contains(groupSize + "")) {
        log.warn("Group size disagreement " + groupSize + " " + groupSizes
            + ", cannot gather tablet and notification partitioning info.");
        setPartitionInfo(null); // disable this worker from processing notifications
        scheduleRetry();
        return;
      }

      List<Bytes> zkSplits = new ArrayList<>();
      SerializedSplits.deserialize(zkSplits::add, zkSplitData);

      Collection<TabletRange> tabletRanges = TabletRange.toTabletRanges(zkSplits);
      PartitionInfo newPI = getGroupInfo(me, children, tabletRanges, groupSize);

      setPartitionInfo(newPI);
    } catch (InterruptedException e) {
      log.debug("Interrupted while gathering tablet and notification partitioning info.", e);
    } catch (Exception e) {
      log.warn("Problem gathering tablet and notification partitioning info.", e);
      setPartitionInfo(null); // disable this worker from processing notifications
      scheduleRetry();
    }
  }

  private synchronized void scheduleRetry() {
    schedExecutor.schedule(() -> updatePartitionInfo(), retrySleepTime, TimeUnit.MILLISECONDS);
    retrySleepTime =
        Math.min(maxSleepTime,
            (long) (1.5 * retrySleepTime) + (long) (retrySleepTime * Math.random()));
  }

  private synchronized void scheduleUpdate() {
    schedExecutor.schedule(() -> updatePartitionInfo(), 0, TimeUnit.MILLISECONDS);
  }

  private class CheckTabletsTask implements Runnable {
    @Override
    public void run() {
      try {

        String me = myESNode.getActualPath();
        while (me == null) {
          UtilWaitThread.sleep(100);
          me = myESNode.getActualPath();
        }
        me = ZKPaths.getNodeFromPath(me);

        String me2 = me;
        boolean imFirst =
            childrenCache.getCurrentData().stream().map(ChildData::getPath)
                .map(ZKPaths::getNodeFromPath).sorted().findFirst().map(s -> s.equals(me2))
                .orElse(false);

        if (imFirst) {

          ChildData childData = childrenCache.getCurrentData(ZookeeperPath.FINDERS + "/splits");
          if (childData == null) {
            byte[] currSplitData = SerializedSplits.serializeTableSplits(env);

            curator.create().forPath(ZookeeperPath.FINDERS + "/splits", currSplitData);
          } else {
            HashSet<Bytes> zkSplits = new HashSet<>();
            SerializedSplits.deserialize(zkSplits::add, childData.getData());

            HashSet<Bytes> currentSplits = new HashSet<>();
            byte[] currSplitData = SerializedSplits.serializeTableSplits(env);
            SerializedSplits.deserialize(currentSplits::add, currSplitData);

            if (!currentSplits.equals(zkSplits)) {
              curator.setData().forPath(ZookeeperPath.FINDERS + "/splits", currSplitData);
            }
          }
        }
      } catch (Exception e) {
        // TODO log
        e.printStackTrace();
      }
    }
  }

  ParitionManager(Environment env, long minSleepTime, long maxSleepTime) {
    try {
      this.curator = env.getSharedResources().getCurator();
      this.env = env;

      this.minSleepTime = minSleepTime;
      this.maxSleepTime = maxSleepTime;
      this.retrySleepTime = minSleepTime;

      groupSize =
          env.getConfiguration().getInt(FluoConfigurationImpl.WORKER_PARTITION_GROUP_SIZE,
              FluoConfigurationImpl.WORKER_PARTITION_GROUP_SIZE_DEFAULT);

      myESNode =
          new PersistentEphemeralNode(curator, Mode.EPHEMERAL_SEQUENTIAL, ZookeeperPath.FINDERS
              + "/f-", ("" + groupSize).getBytes(UTF_8));
      myESNode.start();
      myESNode.waitForInitialCreate(1, TimeUnit.MINUTES);

      childrenCache = new PathChildrenCache(curator, ZookeeperPath.FINDERS, true);
      childrenCache.getListenable().addListener(new FindersListener());
      childrenCache.start(StartMode.BUILD_INITIAL_CACHE);

      schedExecutor =
          Executors.newScheduledThreadPool(1,
              new FluoThreadFactory("Fluo worker partition manager"));
      schedExecutor.scheduleWithFixedDelay(new CheckTabletsTask(), 0, 5, TimeUnit.MINUTES); // TODO

      scheduleUpdate();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void setPartitionInfo(PartitionInfo pi) {
    synchronized (this) {
      if (pi == null || !pi.equals(this.partitionInfo)) {
        this.paritionSetTime = System.nanoTime();
        this.partitionInfo = pi;
        this.notifyAll();
      }

      if (pi != null) {
        retrySleepTime = minSleepTime;
      }
    }
    log.debug("Set partition info : " + pi);
  }

  synchronized PartitionInfo waitForPartitionInfo() throws InterruptedException {
    while (partitionInfo == null
        || System.nanoTime() - paritionSetTime < Math.min(maxSleepTime, STABILIZE_TIME)) {
      wait(minSleepTime);
    }

    return partitionInfo;
  }

  synchronized PartitionInfo getPartitionInfo() {
    if (System.nanoTime() - paritionSetTime < Math.min(maxSleepTime, STABILIZE_TIME)) {
      return null;
    }

    return partitionInfo;
  }

  public void stop() {
    try {
      myESNode.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    try {
      childrenCache.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    schedExecutor.shutdownNow();
    // TODO wait???
  }

  @VisibleForTesting
  static boolean shouldProcess(Notification notification, int divisor, int remainder) {
    byte[] cfcq = NotificationUtil.encodeCol(notification.getColumn());
    return NotificationHashFilter.accept(ByteUtil.toByteSequence(notification.getRow()),
        new ArrayByteSequence(cfcq), divisor, remainder);
  }

  public boolean shouldProcess(Notification notification) {
    PartitionInfo pi = getPartitionInfo();
    if (pi == null) {
      return false;
    }

    return pi.groupsTablets.getContaining(notification.getRow()) != null
        && shouldProcess(notification, pi.groupSize, pi.idInGroup);
  }
}
