package com.github.tm.glink.examples.util;

import com.github.tm.glink.util.DataSorter;

import java.io.IOException;

public class DataSortExample {
  public static void main(String[] args) throws IOException {
    DataSorter sorter = new DataSorter(0.05,
        0.10,
        10 * 60 * 1000,
        5 * 60 * 1000,
        60 * 1000);
    // 结果全部存储在/resources/splitedData这个文件夹下，在操作后会将这个文件夹删除。
    sorter.spiltRawData("glink-examples/src/main/resources/gps_20161101_0710",
        "glink-examples/src/main/resources/splitedData");
    // 使用/resources/splitedData这个文件夹下的所有文件组合成一个乱序的文件。
    sorter.combineToOneFile("glink-examples/src/main/resources/splitedData", "glink-examples/src/main/resources/processedFile", true);
  }
}
