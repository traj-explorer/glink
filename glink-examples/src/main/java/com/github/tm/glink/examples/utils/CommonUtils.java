package com.github.tm.glink.examples.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    String[] files = file.list();
    if (files == null) return null;
    for (int i = 0; i < files.length; ++i) {
      files[i] = path + File.separator + files[i];
    }
    return files;
  }

  /**
   *
   * @param files
   * @param numThreads
   * @return
   */
  public static List<List<String>> distributionFiles(String[] files, int numThreads) {
    int threadFileNum = files.length / numThreads;
    List<List<String>> res = new ArrayList<>();
    for (int i = 0; i < numThreads; ++i) {
      int start = i * threadFileNum;
      int end = (i + 1) * threadFileNum;
      List<String> threadFile = new ArrayList<>(end - start);
      threadFile.addAll(Arrays.asList(files).subList(start, end));
      res.add(threadFile);
    }
    if (numThreads * threadFileNum < files.length) {
      int start = numThreads * threadFileNum;
      for (int i = start; i < files.length; ++i) {
        res.get(i % numThreads).add(files[i]);
      }
    }
    return res;
  }
}
