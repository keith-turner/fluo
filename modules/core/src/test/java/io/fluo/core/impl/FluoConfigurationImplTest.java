/*
 * Copyright 2015 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.fluo.core.impl;

import io.fluo.api.config.FluoConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class FluoConfigurationImplTest {
  @Test
  public void testBasic() {
    FluoConfiguration conf = new FluoConfiguration();
    conf.setWorkerInstances(3);

    Assert.assertEquals(FluoConfigurationImpl.CW_MIN_THREADS_DEFAULT,
        FluoConfigurationImpl.getNumCWThreads(conf));

    conf.setWorkerInstances(10);

    Assert.assertEquals(5, FluoConfigurationImpl.getNumCWThreads(conf));

    conf.setWorkerInstances(100);

    Assert.assertEquals(FluoConfigurationImpl.CW_MAX_THREADS_DEFAULT,
        FluoConfigurationImpl.getNumCWThreads(conf));

    conf.setProperty(FluoConfigurationImpl.CW_MAX_THREADS_PROP, 40);

    Assert.assertEquals(40, FluoConfigurationImpl.getNumCWThreads(conf));

    conf.setWorkerInstances(3);

    Assert.assertEquals(FluoConfigurationImpl.CW_MIN_THREADS_DEFAULT,
        FluoConfigurationImpl.getNumCWThreads(conf));

    conf.setProperty(FluoConfigurationImpl.CW_MIN_THREADS_PROP, 5);

    Assert.assertEquals(5, FluoConfigurationImpl.getNumCWThreads(conf));
  }
}
