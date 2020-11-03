package com.github.tm.glink.index;

import com.github.tm.glink.features.ClassfiedGrids;
import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;
import java.util.List;

/**
 * @author Yu Liebing
 */
public abstract class GridIndex implements Serializable {

  protected int res;

  public abstract int getRes();

  public abstract long getIndex(double lat, double lng);

  /**
   * Get a list of grid indexes that may generate neighbor pairs with the input point.
   * @param lat Point's latitude.
   * @param lng Point's longitude.
   * @param distance The distance(meters) to get a neighbor pair.
   * @param fullMode Whether to get pairs in a full mode, which is a baseline method.
   * @return The grid indexes related to the input point.
   */
  public abstract List<Long> getRangeIndex(double lat, double lng, double distance, boolean fullMode);

  /**
   * Get the indexes intersecting with the boundary of the input geometry.
   * @param geoObject The geometry to get a boundary.
   * @return Intersected indexes.
   */
  public abstract List<Long> getIntersectIndex(Geometry geoObject);

  /**
   * Get the boundary of the area indicated by the input index.
   * @param index The boundary.
   */
  public abstract void getGeoBoundary(long index);

  /**
   * Get the parent index of the current-index-covered area at a lower resolution based
   * on the resolution of the origin index.
   * @param index The origin index.
   * @return The parent index.
   */
  public abstract long getParent(long index);

  /**
   * Get the parent index of the current-index-covered area at a lower resolution based on <i>res</i>.
   * @param index Current index.
   * @param res Resolution of the parent.
   * @return The parent index
   */
  public abstract long getParent(long index, int res);

  /**
   * Get children indexes of the current-index-covered area at a higher resolution based
   * on the resolution of the origin index.
   * @param index The origin index.
   * @return The children indexes.
   */
  public abstract List<Long> getChildren(long index);

  /**
   * Get the children indexes of the current-index-covered area at a higher resolution based on <i>res</i>.
   * @param index Current index.
   * @param res Resolution of the children.
   * @return The children index
   */
  public abstract List<Long> getChildren(long index, int res);

  /**
   * Get the {@link ClassfiedGrids} covered by the input geometry.
   * @param geometry The geometry used to query grids.
   * @return A {@link ClassfiedGrids} object, having the contained grids and the intersected grids separately.
   */
  public abstract ClassfiedGrids getRelatedGrids(Geometry geometry);

  /**
   * Get a index set of the k-th layer ring centered on the current index.
   * K-ring 0 is defined as the origin index, k-ring 1 is defined as k-ring 0 and all neighboring indices, and so on.
   * @param index Current index value.
   * @param k Distance k.
   * @return A indexes set of the k-th layer ring centered on the current index.
   */
  public abstract List<Long> kRing(long index, int k);
}
