package com.github.tm.glink.index;

import org.junit.Test;

import java.util.List;

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
    for (long child : children) {
      System.out.println(child);
    }
  }

}