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

package org.apache.fluo.accumulo.compaction;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.DefaultCompactionStrategy;
import org.apache.accumulo.tserver.compaction.MajorCompactionReason;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;
import org.apache.fluo.accumulo.summarizer.FluoSummarizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

public class FluoCompactionStrategy extends DefaultCompactionStrategy {

  private static final Logger log = LoggerFactory.getLogger(FluoCompactionStrategy.class);

  private double ntfyWeight;
  private double compactionRatio;

  private List<Summary> summaries = Collections.emptyList();

  @Override
  public void init(Map<String, String> options) {
    super.init(options);

    ntfyWeight = Double.parseDouble(options.getOrDefault("ntfy_weight", "2"));
    compactionRatio = Double.parseDouble(options.getOrDefault("ratio", ".25"));
  }

  private boolean shouldConsider(MajorCompactionRequest request) {
    if (request.getReason() != MajorCompactionReason.NORMAL) {
      return false;
    }

    if (request.getFiles().size() == 1) {
      FileRef file = Iterables.getOnlyElement(request.getFiles().keySet());
      if (file.path().getName().startsWith("A")) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean shouldCompact(MajorCompactionRequest request) {
    return shouldConsider(request);
  }

  @Override
  public void gatherInformation(MajorCompactionRequest request) throws IOException {
    super.gatherInformation(request);

    if (shouldConsider(request)) {
      summaries = request.getSummaries(request.getFiles().keySet(),
          sc -> sc.getClassName().equals(FluoSummarizer.class.getName()));
    }
  }

  @Override
  public CompactionPlan getCompactionPlan(MajorCompactionRequest request) {
    Double ratio = computeRatio(request);

    if (ratio != null) {
      log.info(ratio + " > " + compactionRatio + " : " + (ratio > compactionRatio));
    }

    if (ratio != null && ratio > compactionRatio) {
      CompactionPlan plan = new CompactionPlan();
      plan.inputFiles.addAll(request.getFiles().keySet());
      return plan;
    }

    return super.getCompactionPlan(request);
  }

  private Double computeRatio(MajorCompactionRequest request) {
    FluoSummarizer.Counts counts = null;

    if (summaries.size() == 1) {
      counts = FluoSummarizer.getCounts(summaries.get(0));
    }

    Double ratio = null;

    // TODO consider missing data

    if (counts != null) {
      double dataKeep = counts.ack + counts.data + counts.write;
      double dataTotal = counts.ack + counts.data + counts.delLock + counts.delrlock + counts.lock
          + counts.rlock + counts.txDone + counts.write;
      double ntfy = counts.ntfy;
      double ntfyTotal = counts.ntfy + counts.ntfyDel;

      ratio = (dataKeep + ntfyWeight * ntfy) / (dataTotal + ntfyWeight * ntfyTotal);

      // TODO remove
      log.info("Ratio " + ratio + " " + summaries.get(0).getStatistics() + " "
          + summaries.get(0).getFileStatistics());
    }

    return ratio;
  }
}
