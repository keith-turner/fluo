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

import java.util.List;

class PartitionInfo {

  private final int groupId;
  private final int idInGroup;
  private final int groups;
  private final int groupSize;
  private final int workers;
  private final TabletSet groupsTablets;

  PartitionInfo(int myId, int myGroupId, int myGroupSize, int totalGroups, int totalWorkers,
      List<TabletRange> groupsTablets) {
    this.idInGroup = myId;
    this.groupId = myGroupId;
    this.groupSize = myGroupSize;
    this.groups = totalGroups;
    this.workers = totalWorkers;
    this.groupsTablets = new TabletSet(groupsTablets);
  }

  public int getGroupId() {
    return groupId;
  }

  public int getIdInGroup() {
    return idInGroup;
  }

  public int getGroups() {
    return groups;
  }

  public int getGroupSize() {
    return groupSize;
  }

  public int getWorkers() {
    return workers;
  }

  public TabletSet getGroupsTablets() {
    return groupsTablets;
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
