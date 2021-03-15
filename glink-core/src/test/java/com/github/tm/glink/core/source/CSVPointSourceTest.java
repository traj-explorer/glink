package com.github.tm.glink.core.source;
import org.junit.Test;

import java.io.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @author Wang Haocheng
 * @date 2021/3/3 - 11:00 上午
 */
public class CSVPointSourceTest {
    @Test
    public void speedUpTest(){
        LocalDateTime start = LocalDateTime.now();

    }

    @Test
    public void dataCleanTest() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(new File("/Users/haocheng/Downloads/taxiGps20190606-1.csv")));
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File("/Users/haocheng/Downloads/editedFile.csv")));
        String line = null;
        StringBuilder builder = new StringBuilder();
        line = br.readLine();
        while ((line=br.readLine())!=null) {
            String[] items = line.split(",");
            String formattedTime = toCorrectFormat(items[3],"-"," ",":");
            long timestamp = LocalDateTime.parse(formattedTime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
            items[3] = String.valueOf(timestamp);
            for (String item : items) {
                builder.append(item + ',');
            }
            builder.deleteCharAt(builder.length()-1);
            bw.write(builder.toString());
            bw.newLine();
            builder.setLength(0);
        }
        bw.close();
        br.close();
    }
    private static String toCorrectFormat(String input, String dateSpliter, String dateTimeSpliter, String timeSpliter) {
        String[] dateAndTime = input.split(dateTimeSpliter);
        // for date:
        String[] dateInfo = dateAndTime[0].split("/");
        if (dateInfo[1].length() == 1)
            dateInfo[1] = '0' + dateInfo[1];
        if (dateInfo[2].length() == 1)
            dateInfo[2] = '0' + dateInfo[2];
        if (dateAndTime.length ==1){
            return dateInfo[0] + dateSpliter + dateInfo[1] + dateSpliter + dateInfo[2]
                    + dateTimeSpliter
                    + "00:00:00";
        }
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
