/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.core.metrics;

import java.util.List;

import com.codahale.metrics.MetricRegistry;
import org.apache.commons.configuration.Configuration;

public interface ReporterStarter {

  public static interface Params {
    /**
     * 
     * @return Fluo's configuration
     */
    public Configuration getConfiguration();

    /**
     * 
     * @return The metric registry used by Fluo
     */
    public MetricRegistry getMetricRegistry();

    public String getDomain();
  }

  /**
   * A fluo extension point that allows configuration of metrics reporters. This method should configure and start reporters.
   * 
   * @param params
   *          Elements of Fluo environemnt needed to setup reporters.
   * @return A list of closables which represent reporters.
   */
  public List<AutoCloseable> start(Params params);

}
