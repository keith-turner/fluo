package io.fluo.metrics.config;
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



import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.sun.jersey.core.util.Base64;
import io.dropwizard.configuration.ConfigurationSourceProvider;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.core.metrics.ReporterStarter;

public class ReporterStarterImpl implements ReporterStarter {

  @Override
  public List<AutoCloseable> start(Params params) {

    String base64 = params.getConfiguration().getString(FluoConfiguration.METRICS_YAML_BASE64, "");
    final byte[] yaml = Base64.decode(base64);

    ConfigurationSourceProvider csp = new ConfigurationSourceProvider() {
      @Override
      public InputStream open(String path) throws IOException {
        return new ByteArrayInputStream(yaml);
      }
    };

    try {
      return Reporters.startReporters(params.getMetricRegistry(), csp, "", params.getDomain());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
