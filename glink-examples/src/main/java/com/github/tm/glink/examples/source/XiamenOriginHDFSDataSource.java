package com.github.tm.glink.examples.source;

import com.github.tm.glink.core.source.CSVGeoObjectSource;
import com.github.tm.glink.examples.query.KNNQueryJob;
import com.github.tm.glink.features.Point;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @author Wang Haocheng
 * @date 2021/2/7 - 11:07 下午
 */
public class XiamenOriginHDFSDataSource extends CSVGeoObjectSource<Point>{
    private String hdfsUrl;

    public XiamenOriginHDFSDataSource(String path, String hdfsurl) throws IOException {
        super(path);
        this.hdfsUrl = hdfsurl;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", hdfsUrl);
        FileSystem fs = FileSystem.newInstance(conf);
        formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        FSDataInputStream fsDataInputStream = fs.open(new Path(filePath));
        bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
    }
    @Override
    public Point parseLine(String line) {
        try {
            String[] items = line.split(",");
            String carNo = items[6];
            Double lat = Double.parseDouble(items[5]);
            Double lng = Double.parseDouble(items[4]);
            long timestamp = Long.parseLong(items[3]);
            return new Point(carNo, lat, lng, timestamp);
        } catch (Exception e) {
            System.out.println("该行格式错误： " + line);
            return null;
        }
    }

}
