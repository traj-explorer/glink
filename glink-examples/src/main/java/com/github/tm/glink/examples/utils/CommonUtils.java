package com.github.tm.glink.examples.utils;

import java.io.File;

/**
 * @author Yu Liebing
 * */
public class CommonUtils {
  public static int getThreadNum(int fileNum) {
    int maxThreads = Runtime.getRuntime().availableProcessors() + 1;
    return Math.min(fileNum, maxThreads);
  }

  public static String[] listFiles(String path) {
    File file = new File(path);
    return file.list();
  }
}
