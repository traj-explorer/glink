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

    private Schema schema;
    private GenericRecord genericRecord;
    private TileResult<V> tileResult;
    protected Injection<GenericRecord, byte[]> injection;



    public AvroTileResult(TileResult<V> tileResult) {
        this.tileResult = tileResult;
        this.schema = new Schema.Parser().parse(SCHEMA);
        genericRecord = new GenericData.Record(schema);
        injection = GenericAvroCodecs.toBinary(this.schema);
    }

    public byte[] serialize() throws IOException {
        ArrayList<Integer> pixNos = new ArrayList<>();
        ArrayList<Integer> values = new ArrayList<>();
        int i =0;
        for (PixelResult<V> pixelResult : tileResult.getGridResult()) {
            pixNos.add(pixelResult.getPixel().getPixelNo());
            values.add((Integer) pixelResult.getResult());
        }
        genericRecord.put("pixNos", pixNos);
        genericRecord.put("values", values);
        ArrayList<Integer> pixNos2 = (ArrayList<Integer>) genericRecord.get("pixNos");
        return injection.apply(genericRecord);
    }

    public TileResult deserialize(byte[] data) {
        GenericRecord record = injection.invert(data).get();
        return genericToTileResult(record);
    }

    public TileResult<V> genericToTileResult(GenericRecord record) {
        GenericData.Array<Integer> pixNos = (GenericData.Array<Integer>) record.get("pixNos");
        GenericData.Array<Integer> values = (GenericData.Array<Integer>)  record.get("values");
        List<PixelResult<Integer>> list = new ArrayList<>();
        Iterator pixNosIter = pixNos.iterator();
        Iterator valuesIter = values.iterator();
        while (pixNosIter.hasNext()) {
           list.add((PixelResult<Integer>) new PixelResult<>(new Pixel(tileResult.getTile(), (Integer) pixNosIter.next()), (int)valuesIter.next()));
        }
        TileResult ret = new TileResult(tileResult.getTile());
        ret.setGridResult(list);
        return ret;
    }

    public static void main(String[] args) throws IOException {
        ArrayList<Integer> pixNos = new ArrayList<>();
        ArrayList<Integer> values = new ArrayList<>();
        for ( int i = 0; i < 256 * 256; i++) {
            pixNos.add(i);
            values.add(i);
        }
        TileResult<java.lang.Integer> tileResult = new TileResult<>(new Tile(1,4,5));
        List<PixelResult<Integer>> list = new ArrayList<>();
        for( int i = 0; i < pixNos.size(); i++) {
            list.add((PixelResult<Integer>) new PixelResult<>(new Pixel(tileResult.getTile(), pixNos.get(i)), values.get(i)));
        }
        TileResult ret = new TileResult(tileResult.getTile());
        ret.setGridResult(list);
//        System.out.println(ret.toString());
        AvroTileResult avroTileResult = new AvroTileResult(ret);
        System.out.println(avroTileResult.serialize().length);
    }
}
