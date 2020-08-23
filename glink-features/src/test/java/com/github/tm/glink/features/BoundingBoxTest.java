package com.github.tm.glink.features;

import org.junit.Test;

public class BoundingBoxTest {

  @Test
  public void getBufferBBoxTest() {
    BoundingBox bbox = new BoundingBox(10, 20, 40, 50);
    BoundingBox bufferBBox = bbox.getBufferBBox(1000);

    System.out.println(bbox);
    System.out.println(bufferBBox);
  }

  @Test
  public void getBufferBBoxPerformanceTest() {
    BoundingBox bbox = new BoundingBox(10, 20, 40, 50);
    long start = System.currentTimeMillis();
    for (int i = 0; i < 10000; ++i) {
      BoundingBox bufferBBox = bbox.getBufferBBox(1000);
    }
    long end = System.currentTimeMillis();
    System.out.println(end - start);
  }
}