package com.github.tm.glink.core.tile;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Wang Haocheng
 * @date 2021/4/16 - 8:32 下午
 */
public class AvroTileResult<V> implements Serializable {

    public static final String SCHEMA = "{" +
            "\"type\": \"record\" ," +
            "  \"name\": \"TileResult\"," +
            "  \"fields\": [\n" +
            "    {\"name\": \"pixNos\", \"type\": { \"type\": \"array\", \"items\": \"int\"}},\n" +
            "    {\"name\": \"values\", \"type\": { \"type\": \"array\", \"items\": \"int\"}}\n" +
            "]}";

    private static Schema schema = new Schema.Parser().parse(SCHEMA);
    protected static Injection<GenericRecord, byte[]> injection = GenericAvroCodecs.toBinary(schema);

    private Tile tile;

    public AvroTileResult(Tile tile) {
        this.tile = tile;
    }

    public static byte[] serialize(TileResult tileResult) throws IOException {
        ArrayList<Integer> pixNos = new ArrayList<>();
        ArrayList<Integer> values = new ArrayList<>();
        int i =0;
        for (Object pixelResult : tileResult.getGridResult()) {
            pixNos.add(((PixelResult)pixelResult).getPixel().getPixelNo());
            values.add((Integer) ((PixelResult)pixelResult).getResult());
        }
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("pixNos", pixNos);
        genericRecord.put("values", values);
        return injection.apply(genericRecord);
    }

    public TileResult<V> deserialize(byte[] data) {
        GenericRecord record = injection.invert(data).get();
        return genericToTileResult(record);
    }

    private TileResult<V> genericToTileResult(GenericRecord record) {
        GenericData.Array<Integer> pixNos = (GenericData.Array<Integer>) record.get("pixNos");
        GenericData.Array<Integer> values = (GenericData.Array<Integer>)  record.get("values");
        List<PixelResult<Integer>> list = new ArrayList<>();
        Iterator<Integer> pixNosIter = pixNos.iterator();
        Iterator<Integer> valuesIter = values.iterator();
        while (pixNosIter.hasNext()) {
           list.add((PixelResult<Integer>) new PixelResult<>(new Pixel(tile, (Integer) pixNosIter.next()), (int)valuesIter.next()));
        }
        TileResult ret = new TileResult(tile);
        ret.setGridResult(list);
        return ret;
    }
}
