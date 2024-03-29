package com.github.tm.glink.core.datastream;

import com.github.tm.glink.core.enums.TextFileSplitter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.Instant;

/**
 * @author Wang Haocheng
 * @date 2021/4/22 - 8:09 下午
 */
public class CSVStringSourceSimulation extends RichSourceFunction<String> {

    protected String filePath;
    protected BufferedReader bufferedReader;

    // Variables for speed up the simulated stream.
    private Integer speedFactor;
    private long startTime;
    private long startEventTime = -1;
    private long preEventTime;
    private int syncCounter;
    private int timeFieldIndex;
    private TextFileSplitter splitter;
    private boolean withPid; // 如果没有pid，我们需要自行在后面附加。
    private int pid;

    public CSVStringSourceSimulation(String filePath, int speedFactor, int timeFieldIndex, TextFileSplitter splitter, boolean withPid) {
        this.filePath = filePath;
        this.speedFactor = speedFactor;
        this.timeFieldIndex = timeFieldIndex;
        this.startTime = Instant.now().toEpochMilli();
        this.splitter = splitter;
        this.withPid = withPid;
    }


    protected void checkTimeAndWait(String line) throws InterruptedException {

        if (timeFieldIndex == -1) {
            return;
        }
        String time = line.split(splitter.getDelimiter())[timeFieldIndex];
        long thisEventTime = Long.parseLong(time);

        if (startEventTime < 0) {
            startEventTime = thisEventTime;
            preEventTime = thisEventTime;
        } else {
            long gapTime = (thisEventTime - preEventTime) / speedFactor;
            if (gapTime > 0 || syncCounter > 1000) {
                long currentTime = System.currentTimeMillis();
                long targetEmitTime = (long) ((thisEventTime - startEventTime) / speedFactor) + startTime;
                long waitTime = targetEmitTime - currentTime;
                if (waitTime > 0) {
                    try {
                        Thread.sleep(waitTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                syncCounter = 0;
            } else {
                syncCounter++;
            }
        }
        preEventTime = thisEventTime;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        FileReader fileReader = new FileReader(filePath);
        bufferedReader = new BufferedReader(fileReader);
    }

    @Override
    public final void run(SourceContext<String> sourceContext) throws Exception {
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            if (speedFactor > 0)
                checkTimeAndWait(line);
            if (!withPid) {
                sourceContext.collect(line + "," + pid);
                pid++;
            } else {
                sourceContext.collect(line);
            }
        }
    }

    @Override
    public final void cancel() {

    }

    @Override
    public final void close() throws Exception {
        bufferedReader.close();
    }
}
