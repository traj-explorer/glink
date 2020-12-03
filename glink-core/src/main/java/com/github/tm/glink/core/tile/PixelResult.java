package com.github.tm.glink.core.tile;

/**
 * @author Yu Liebing
 */
public class PixelResult<V> {

  private Pixel pixel;
  private V result;

  public PixelResult(Pixel pixel, V result) {
    this.pixel = pixel;
    this.result = result;
  }

  public Pixel getPixel() {
    return pixel;
  }

  public void setPixel(Pixel pixel) {
    this.pixel = pixel;
  }

  public V getResult() {
    return result;
  }

  public void setResult(V result) {
    this.result = result;
  }
}
