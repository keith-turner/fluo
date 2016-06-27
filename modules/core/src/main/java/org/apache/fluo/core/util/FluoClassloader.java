package org.apache.fluo.core.util;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;

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

    if(name.startsWith("org.apache.fluo.api") || name.startsWith("java") || name.startsWith("com.sun")) {
      return super.loadClass(name, resolve);
    } else {
      return findClass(name);
    }
  }

  public static URL[] parseConfig(String classpath) throws MalformedURLException {

    ArrayList<URL> urls = new ArrayList<>();

    for(String path : classpath.split(File.pathSeparator)){
      if(path.endsWith("*")){
        File[] jars = new File(path.substring(0, path.length() - 1)).listFiles(new FilenameFilter() {
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
  }
}
