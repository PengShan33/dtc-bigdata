package com.dtc.analytic.worker;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * @author
 */
public class FileDataSource extends RichSourceFunction<String> {

    private volatile Boolean isRunning = true;

    @Override
    public void run(SourceContext<String> context) throws Exception {
        BufferedReader bufferedReader = new BufferedReader(new FileReader("C:\\Users\\wh\\Desktop\\router\\h3c-router-data.txt"));
        while (isRunning) {
            String content = bufferedReader.readLine();
            if (StringUtils.isBlank(content)) {
                continue;
            }
            context.collect(content);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
