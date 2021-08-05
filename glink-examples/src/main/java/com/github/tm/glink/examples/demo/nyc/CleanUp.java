package com.github.tm.glink.examples.demo.nyc;

import com.github.tm.glink.examples.demo.xiamen.HBaseCatalogCleaner;

/**
 * @author Wang Haocheng
 * @date 2021/6/20 - 7:23 下午
 */
public class CleanUp {
  public static final String CATALOG_NAME = "NYC";
  public static final String TILE_SCHEMA_NAME = "Heatmap";
  public static final String POINTS_SCHEMA_NAME = "JoinedPoints";

  public static void main(String[] args) {
    // Drop old tables in HBase
    new HBaseCatalogCleaner(Heatmap.ZOOKEEPERS).deleteTable(CATALOG_NAME, TILE_SCHEMA_NAME);
    new HBaseCatalogCleaner(Heatmap.ZOOKEEPERS).deleteTable(CATALOG_NAME, POINTS_SCHEMA_NAME);
    System.out.println("********* Cleanup tables finished *********");
  }
}

