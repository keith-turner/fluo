package org.apache.fluo.core.util;

import java.net.URL;
import java.net.URLClassLoader;

public class FluoClassloader extends URLClassLoader {

  //TODO resources...

  public FluoClassloader(URL[] urls) {
    super(urls);
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    Class<?> c = findLoadedClass(name);
    if (c != null)
      return c;

    if(name.startsWith("org.apache.fluo.api") || name.startsWith("java")) {
      return super.loadClass(name, resolve);
    } else {
      return findClass(name);
    }
  }
}
