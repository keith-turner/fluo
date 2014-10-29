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
package io.fluo.api.config;

import java.io.File;
import java.util.NoSuchElementException;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link FluoConfiguration}
 */
public class FluoConfigurationTest {
  
  private FluoConfiguration base = new FluoConfiguration();
  
  @Test
  public void testDefaults() {
    Assert.assertEquals(FluoConfiguration.CLIENT_ZOOKEEPER_CONNECT_DEFAULT, base.getZookeepers());
    Assert.assertEquals(FluoConfiguration.CLIENT_ZOOKEEPER_TIMEOUT_DEFAULT, base.getZookeeperTimeout());
    Assert.assertEquals(FluoConfiguration.CLIENT_ACCUMULO_ZOOKEEPERS_DEFAULT, base.getAccumuloZookeepers());
    Assert.assertEquals(FluoConfiguration.CLIENT_CLASS_DEFAULT, base.getClientClass());
    Assert.assertEquals(FluoConfiguration.ADMIN_ALLOW_REINITIALIZE_DEFAULT, base.getAllowReinitialize());
    Assert.assertEquals(FluoConfiguration.ADMIN_CLASS_DEFAULT, base.getAdminClass());
    Assert.assertEquals(FluoConfiguration.ADMIN_ACCUMULO_CLASSPATH_DEFAULT, base.getAccumuloClasspath());
    Assert.assertEquals(FluoConfiguration.WORKER_NUM_THREADS_DEFAULT, base.getWorkerThreads());
    Assert.assertEquals(FluoConfiguration.WORKER_INSTANCES_DEFAULT, base.getWorkerInstances());
    Assert.assertEquals(FluoConfiguration.WORKER_MAX_MEMORY_MB_DEFAULT, base.getWorkerMaxMemory());
    Assert.assertEquals(FluoConfiguration.WORKER_NUM_CORES_DEFAULT, base.getWorkerNumCores());
    Assert.assertEquals(FluoConfiguration.TRANSACTION_ROLLBACK_TIME_DEFAULT, base.getTransactionRollbackTime());
    Assert.assertEquals(FluoConfiguration.LOADER_NUM_THREADS_DEFAULT, base.getLoaderThreads());
    Assert.assertEquals(FluoConfiguration.LOADER_QUEUE_SIZE_DEFAULT, base.getLoaderQueueSize());
    Assert.assertEquals(FluoConfiguration.ORACLE_PORT_DEFAULT, base.getOraclePort());
    Assert.assertEquals(FluoConfiguration.ORACLE_INSTANCES_DEFAULT, base.getOracleInstances());
    Assert.assertEquals(FluoConfiguration.ORACLE_MAX_MEMORY_MB_DEFAULT, base.getOracleMaxMemory());
    Assert.assertEquals(FluoConfiguration.ORACLE_NUM_CORES_DEFAULT, base.getOracleNumCores());
    Assert.assertEquals(FluoConfiguration.MINI_CLASS_DEFAULT, base.getMiniClass());
    Assert.assertEquals(FluoConfiguration.METRICS_YAML_BASE64_DEFAULT, base.getMetricsYamlBase64());
  }
      
  @Test(expected = NoSuchElementException.class)
  public void testInstance() {
    base.getAccumuloInstance();
  }
  
  @Test(expected = NoSuchElementException.class)
  public void testUser() {
    base.getAccumuloUser();
  }
  
  @Test(expected = NoSuchElementException.class)
  public void testPassword() {
    base.getAccumuloPassword();
  }
  
  @Test(expected = NoSuchElementException.class)
  public void testTable() {
    base.getAccumuloTable();
  }
  
  @Test
  public void testSetGet() {
    FluoConfiguration config = new FluoConfiguration();
    Assert.assertEquals("path1,path2", config.setAccumuloClasspath("path1,path2").getAccumuloClasspath());
    Assert.assertEquals("instance", config.setAccumuloInstance("instance").getAccumuloInstance());
    Assert.assertEquals("pass", config.setAccumuloPassword("pass").getAccumuloPassword());
    Assert.assertEquals("table", config.setAccumuloTable("table").getAccumuloTable());
    Assert.assertEquals("user", config.setAccumuloUser("user").getAccumuloUser());
    Assert.assertEquals("admin", config.setAdminClass("admin").getAdminClass());
    Assert.assertTrue(config.setAllowReinitialize(true).getAllowReinitialize());
    Assert.assertEquals("client", config.setClientClass("client").getClientClass());
    Assert.assertEquals(4, config.setLoaderQueueSize(4).getLoaderQueueSize());
    Assert.assertEquals(7, config.setLoaderThreads(7).getLoaderThreads());
    Assert.assertEquals("mini", config.setMiniClass("mini").getMiniClass());
    Assert.assertEquals(8, config.setOracleMaxMemory(8).getOracleMaxMemory());
    Assert.assertEquals(9, config.setOraclePort(9).getOraclePort());
    Assert.assertEquals(10, config.setOracleInstances(10).getOracleInstances());
    Assert.assertEquals(11, config.setWorkerInstances(11).getWorkerInstances());
    Assert.assertEquals(12, config.setWorkerMaxMemory(12).getWorkerMaxMemory());
    Assert.assertEquals(13, config.setWorkerThreads(13).getWorkerThreads());
    Assert.assertEquals("zoos1", config.setZookeepers("zoos1").getZookeepers());
    Assert.assertEquals("zoos2", config.setAccumuloZookeepers("zoos2").getAccumuloZookeepers());
    Assert.assertEquals(14, config.setZookeeperTimeout(14).getZookeeperTimeout());
    Assert.assertEquals(15, config.setWorkerNumCores(15).getWorkerNumCores());
    Assert.assertEquals(16, config.setOracleNumCores(16).getOracleNumCores());
  }
  
  @Test
  public void testHasClientProps() {
    FluoConfiguration config = new FluoConfiguration();
    Assert.assertFalse(config.hasRequiredClientProps());
    config.setAccumuloUser("user");
    Assert.assertFalse(config.hasRequiredClientProps());
    config.setAccumuloPassword("pass");
    Assert.assertFalse(config.hasRequiredClientProps());
    config.setAccumuloInstance("instance");
    Assert.assertTrue(config.hasRequiredClientProps());
  }
  
  @Test
  public void testHasAdminProps() {
    FluoConfiguration config = new FluoConfiguration();
    Assert.assertFalse(config.hasRequiredAdminProps());
    config.setAccumuloUser("user");
    config.setAccumuloPassword("pass");
    config.setAccumuloInstance("instance");
    config.setAccumuloTable("table");
    Assert.assertTrue(config.hasRequiredAdminProps());
  }
  
  @Test
  public void testHasWorkerProps() {
    FluoConfiguration config = new FluoConfiguration();
    Assert.assertFalse(config.hasRequiredWorkerProps());
    config.setAccumuloUser("user");
    config.setAccumuloPassword("pass");
    config.setAccumuloInstance("instance");
    Assert.assertTrue(config.hasRequiredWorkerProps());
  }
  
  @Test
  public void testHasOracleProps() {
    FluoConfiguration config = new FluoConfiguration();
    Assert.assertFalse(config.hasRequiredOracleProps());
    config.setAccumuloUser("user");
    config.setAccumuloPassword("pass");
    config.setAccumuloInstance("instance");
    Assert.assertTrue(config.hasRequiredOracleProps());
  }
  
  @Test
  public void testHasMiniFluoProps() {
    FluoConfiguration config = new FluoConfiguration();
    Assert.assertFalse(config.hasRequiredMiniFluoProps());
    config.setAccumuloUser("user");
    config.setAccumuloPassword("pass");
    config.setAccumuloInstance("instance");
    config.setAccumuloTable("table");
    Assert.assertTrue(config.hasRequiredMiniFluoProps());
  }
  
  @Test
  public void testLoadingPropsFile() {
    File propsFile = new File("../distribution/src/main/config/fluo.properties");
    Assert.assertTrue(propsFile.exists());
    
    FluoConfiguration config = new FluoConfiguration(propsFile);
    // make sure classpath contains comma.  otherwise it was shortened
    Assert.assertTrue(config.getAccumuloClasspath().contains(","));
    // check for values set in prop file
    Assert.assertEquals("localhost/fluo", config.getZookeepers());
    Assert.assertEquals("localhost", config.getAccumuloZookeepers());
    Assert.assertEquals("", config.getAccumuloUser());
    Assert.assertEquals("", config.getAccumuloPassword());
    Assert.assertEquals("", config.getAccumuloTable());
  }

  @Test
  public void testMetricsProp() throws Exception {
    FluoConfiguration config = new FluoConfiguration();
    config.setMetricsYaml(getClass().getClassLoader().getResourceAsStream("metrics.yaml"));
    Assert.assertEquals("LS0tCmZyZXF1ZW5jeTogMTAgc2Vjb25kcwpyZXBvcnRlcnM6CiAgLSB0eXBlOiBjb25zb2xlCg==", config.getMetricsYamlBase64());
    byte[] bytes1 = IOUtils.toByteArray(getClass().getClassLoader().getResourceAsStream("metrics.yaml"));
    byte[] bytes2 = IOUtils.toByteArray(config.getMetricsYaml());
    Assert.assertArrayEquals(bytes1, bytes2);
  }
}
