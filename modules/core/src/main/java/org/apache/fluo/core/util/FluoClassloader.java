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

package org.apache.fluo.core.util;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;

import org.apache.commons.configuration.Configuration;


public class FluoClassloader extends URLClassLoader {

  // TODO resources...

  public FluoClassloader(URL[] urls) {
    super(urls);
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve)
      throws ClassNotFoundException {
    Class<?> c = findLoadedClass(name);
    if (c != null)
      return c;

    if (name.startsWith("org.apache.fluo.api") || name.startsWith("java")
        || name.startsWith("com.sun") || name.startsWith("org.apache.commons.configuration")) {
      return super.loadClass(name, resolve);
    } else {
      return findClass(name);
    }
  }

  public static URL[] parseClasspath(String classpath) {

    try {
      ArrayList<URL> urls = new ArrayList<>();

      for (String path : classpath.split(File.pathSeparator)) {
        if (path.endsWith("*")) {
          File[] jars =
              new File(path.substring(0, path.length() - 1)).listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                  return name.endsWith(".jar") && new File(dir, name).isFile();
                }
              });

          for (File file : jars) {
            urls.add(file.toURI().toURL());
          }
        } else {
          urls.add(new File(path).toURI().toURL());
        }
      }

      return urls.toArray(new URL[urls.size()]);
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  public static ClassLoader getInstance(Configuration config) {
    // TODO constant
    String classpath = config.getString("org.apache.fluo.appClasspath");
    if (classpath != null && !classpath.trim().isEmpty()) {
      URL[] urls = FluoClassloader.parseClasspath(classpath);
      return new FluoClassloader(urls);
    } else {
      return FluoClassloader.class.getClassLoader();
    }
  }
}
