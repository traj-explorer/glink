package com.github.tm.glink.index;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class H3IndexTest {

  private GridIndex gridIndex = new H3Index(12);

  @Test
  public void hierarchicalTest() {
    double lat = 35.50, lng = 114.45;
    long index = gridIndex.getIndex(lat, lng);
    System.out.println("index: " + index);
    long parent = gridIndex.getParent(index);
    System.out.println("parent: " + parent);
    List<Long> children = gridIndex.getChildren(parent);
    System.out.println("children: ");
    for (long child : children) {
      System.out.println(child);
    }
  }

  @Test
  public void kRingTest() {
    double lat = 35.50, lng = 114.45;
    long index = gridIndex.getIndex(lat, lng);
    System.out.println("index: " + index);

    List<Long> kRings = gridIndex.kRing(index, 1);
    for (long ring : kRings) {
      System.out.println(ring);
    }
  }

  @Test
  public void test() {
    System.out.println("1".hashCode());
    System.out.println("2".hashCode());
  }
}