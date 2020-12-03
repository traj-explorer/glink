package com.github.tm.glink.core.enums;

import java.io.Serializable;

/**
 * @author Yu Liebing
 */
public enum TextFileSplitter implements Serializable {
  /**
   * The csv.
   */
  CSV(","),

  /**
   * The tsv.
   */
  TSV("\t"),

  /**
   * The geojson.
   */
  GEOJSON(""),

  /**
   * The wkt.
   */
  WKT("\t"),

  /**
   * The wkb.
   */
  WKB("\t"),

  COMMA(","),

  TAB("\t"),

  QUESTIONMARK("?"),

  SINGLEQUOTE("'"),

  QUOTE("\""),

  UNDERSCORE("_"),

  DASH("-"),

  PERCENT("%"),

  TILDE("~"),

  PIPE("|"),

  SEMICOLON(";");

  /**
   * Gets the file data splitter.
   *
   * @param str the str
   * @return the file data splitter
   */
  public static TextFileSplitter getTextFileSplitter(String str) {
    for (TextFileSplitter me : TextFileSplitter.values()) {
      if (me.getDelimiter().equalsIgnoreCase(str) || me.name().equalsIgnoreCase(str)) {
        return me;
      }
    }
    throw new IllegalArgumentException("[" + TextFileSplitter.class + "] Unsupported FileDataSplitter:" + str);
  }

  /**
   * The splitter.
   */
  private final String splitter;

  /**
   * Instantiates a new file data splitter.
   *
   * @param splitter the splitter
   */
  TextFileSplitter(String splitter) {
    this.splitter = splitter;
  }

  /**
   * Gets the delimiter.
   *
   * @return the delimiter
   */
  public String getDelimiter() {
    return this.splitter;
  }
}
