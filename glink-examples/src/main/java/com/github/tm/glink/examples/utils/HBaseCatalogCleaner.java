package com.github.tm.glink.examples.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author Wang Haocheng
 * @date 2021/3/15 - 9:45 下午
 */
public class HBaseCatalogCleaner {
    public static void clean(String HBaseCatalogPrefix) throws IOException {
        String bashCommand = "sh " + "glink-examples/src/main/java/com/github/tm/glink/examples/utils/HBaseCatalogClean " + HBaseCatalogPrefix;
        Process process = Runtime.getRuntime().exec(bashCommand);
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line ="";
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
    }
}
