package com.github.tm.glink.index;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class UGridIndexTest {

  GridIndex gridIndex = new UGridIndex(90.d);

  @Test
  public void getIndexTest() {
    long index = gridIndex.getIndex(1, 1);
    System.out.println(index);
    System.out.println(index >> 30);
  }

  @Test
  public void getRangeIndexTest() {
    List<Long> indexes = gridIndex.getRangeIndex(0, 0, 500, false);
    for (long index : indexes) {
      System.out.println(index);
    }
  }

  @Test
  public void test() {
    long x = 1;
    long y = 2;
    long z = (x << 30) | y;

    System.out.println(z >> 30);
    System.out.println(z & 0xfffffff);
  }

}