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
    private DateTimeFormatter formatter;
    private String hdfsUrl;

    public XiamenOriginHDFSDataSource(String path, String hdfsurl) throws IOException {
        this.filePath = path;
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
            String formattedTime = addZeroPrefix(items[3],"-"," ",":");
            long timestamp = LocalDateTime.parse(formattedTime, formatter).toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
            return new Point(carNo, lat, lng, timestamp);
        } catch (Exception e) {
            System.out.println("该行格式错误： " + line);
            return null;
        }
    }

    private static String addZeroPrefix(String input, String dateSpliter, String dateTimeSpliter, String timeSpliter) {
        String[] dateAndTime = input.split(dateTimeSpliter);
        // for date:
        String[] dateInfo = dateAndTime[0].split("/");
        if (dateInfo[1].length() == 1)
            dateInfo[1] = '0' + dateInfo[1];
        if (dateInfo[2].length() == 1)
            dateInfo[2] = '0' + dateInfo[2];

        // for time
        String[] timeInfo = dateAndTime[1].split(timeSpliter);
        StringBuilder timeBuilder = new StringBuilder();
        for (int i=0; i<3; i++) {
            if (timeInfo[i].length() == 1) {
                if(i == 0) {
                    timeBuilder.append('0'+timeInfo[i]);
                } else {
                    timeBuilder.append(timeSpliter+'0'+timeInfo[i]);
                }
            } else {
                if(i == 0) {
                    timeBuilder.append(timeInfo[i]);
                } else {
                    timeBuilder.append(timeSpliter+timeInfo[i]);
                }
            }
        }
        return dateInfo[0] + dateSpliter + dateInfo[1] + dateSpliter + dateInfo[2]
                + dateTimeSpliter
                + timeBuilder.toString();
    }
}
