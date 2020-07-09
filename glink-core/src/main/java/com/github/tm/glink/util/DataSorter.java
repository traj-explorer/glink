package com.github.tm.glink.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class DataSorter {
  private double late1RecordRate;
  private double late2RecordRate;
  private double delay1TimeLength;
  private double delay2TimeLength;
  private double normalDelayTimeLength;

  public DataSorter(double late1RecordRate, double late2RecordRate, double delay1TimeLength, double delay2TimeLength, double normalDelayTimeLength) {
    this.late1RecordRate = late1RecordRate;
    this.late2RecordRate = late2RecordRate;
    this.delay1TimeLength = delay1TimeLength;
    this.delay2TimeLength = delay2TimeLength;
    this.normalDelayTimeLength = normalDelayTimeLength;
  }

  private long getRandomDelayTime(Random random) {
    double temp = random.nextDouble();
    long delay = 0L;
    if (temp < late1RecordRate) {
      delay =  new Double(random.nextGaussian() * Math.sqrt(delay1TimeLength) + delay1TimeLength).longValue();
    } else if (temp < late2RecordRate) {
      delay = new Double(random.nextGaussian() * Math.sqrt(delay2TimeLength) + delay2TimeLength).longValue();
    } else {
      delay = new Double(random.nextGaussian() * Math.sqrt(normalDelayTimeLength) + normalDelayTimeLength).longValue();
    }
    return delay;
  }

  /**
   * 使用原始的数据集，为其事件时间戳添加随机的延迟要素作为模拟的到达时间，再根据到达时间的值将其划分到不同的数据文件中。
   * @param inputFilePath 原始静态数据数据集的数据文件。
   * @param outputPath 被拆分的各时段数据文件的保存路径。
   */
  public void spiltRawData(String inputFilePath, String outputPath) throws IOException {
    String filepath = inputFilePath;
    File newFIle = new File(filepath);
    BufferedReader reader = new BufferedReader(new FileReader(filepath));
    String line = null;
    long delay = 0L;
    Random random = new Random(500);
    while ((line = reader.readLine()) != null && reader.ready()) {
      long timestamp = Long.parseLong(line.split(",")[2]);
      long arriveTime = timestamp + getRandomDelayTime(random);
      // 以"/1小时/1分钟内数据.txt"为单位进行组织数据记录。其中时间以模拟的延迟时间为。
      long hourStart = (arriveTime / (1000 * 60 * 60)) * (1000 * 60 * 60);
      long minutes = (arriveTime / (1000 * 60)) * (1000 * 60);
      String dictPath = outputPath + File.separatorChar + hourStart;
      String fileName = minutes + ".txt";
      File toProcessFile = new File(dictPath + File.separatorChar + fileName);
      if (toProcessFile.exists()) {
        BufferedWriter out = new BufferedWriter(new FileWriter(toProcessFile, true));
        out.write(line);
        out.newLine();
        out.close();
      } else {
        String temp = null;
        if (toProcessFile.getParentFile().exists()) {
          toProcessFile.getParentFile().delete();
        }
        toProcessFile.getParentFile().mkdirs();
        toProcessFile.createNewFile();
        BufferedWriter out = new BufferedWriter(new FileWriter(toProcessFile, true));
        out.write(line);
        out.newLine();
        out.close();
      }
    }
  }

  /**
   * 将分布在不同文件夹内的各时段数据记录文件按顺序整合到同一个记录文件中。
   * @param dictionaryPath 包含所有不同小时数据的文件夹路径。
   * @param outputFilePath 最终会获得的单个记录文件的路径。
   * @param toDelete 是否在合并每个文件后将该文件删除。
   */
  public void combineToOneFile(String dictionaryPath, String outputFilePath, Boolean toDelete) throws IOException {
    File rootDictionry = new File(dictionaryPath);
    File outputFile = new File(outputFilePath);
    if (outputFile.exists()) {
      outputFile.delete();
      outputFile.createNewFile();
    }
    String line = null;
    BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile, true));
    for (File hourFolder : rootDictionry.listFiles()) {
      for (File recordFile : hourFolder.listFiles()) {
        BufferedReader br = new BufferedReader(new FileReader(recordFile));
        while (br.ready() && (line = br.readLine()) != null) {
          bw.write(line);
          bw.newLine();
        }
        br.close();
      }
    }
    bw.close();
    if (toDelete) {
      deleteDir(rootDictionry.getAbsolutePath());
    }
  }

  public static boolean deleteDir(String path) {
    File file = new File(path);
    //判断是否待删除目录是否存在
    if (!file.exists()) {
      System.err.println("The dir are not exists!");
      return false;
    }
    //取得当前目录下所有文件和文件夹
    String[] content = file.list();
    for (String name : content) {
      File temp = new File(path, name);
      //判断是否是目录
      if (temp.isDirectory()) {
        //递归调用，删除目录里的内容
        deleteDir(temp.getAbsolutePath());
        temp.delete();
      } else {
        //直接删除文件
        if (!temp.delete()) {
        System.err.println("Failed to delete " + name);
        }
      }
    }
    return true;
  }
}
