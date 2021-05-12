package com.github.tm.glink.connector.geomesa.sink;

import com.github.tm.glink.core.tile.TileResult;

/**
 * 不同的TileResult会对应不同的data类型与主键构成方式。
 * @author Wang Haocheng
 */
public interface TileResultToSimpleFeatureConverter extends GeoMesaSimpleFeatureConverter<TileResult> {

  void resultListToOutputFormat(TileResult result);

}
