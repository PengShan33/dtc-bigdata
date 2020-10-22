package com.dtc.alarm.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.opentsdb.client.ExpectResponse;
import org.opentsdb.client.HttpClientImpl;
import org.opentsdb.client.request.QueryBuilder;
import org.opentsdb.client.request.SubQueries;
import org.opentsdb.client.response.SimpleHttpResponse;
import org.opentsdb.client.util.Aggregator;

import java.io.IOException;
import java.util.Map;

/**
 * @author
 */
@Slf4j
public class OpenTSDBUtil {

    /**
     * Query data from openTSDB.
     *
     * @param client
     * @param code   The code of index.
     * @param tag    The ip of host.
     * @param func   max, min, avg
     * @param second 秒
     */
    public static Map<String, Object> queryOpenTSDB(HttpClientImpl client, String code, String tag, String func, int second) throws IOException {

        QueryBuilder builder = QueryBuilder.getInstance();
        SubQueries subQueries = new SubQueries();

        String aggregator = "";
        if (Aggregator.avg.toString().equals(func)) {
            aggregator = Aggregator.avg.toString();
        } else if (Aggregator.max.toString().equals(func)) {
            aggregator = Aggregator.max.toString();
        } else if (Aggregator.min.toString().equals(func)) {
            aggregator = Aggregator.min.toString();
        } else {
            aggregator = Aggregator.avg.toString();
        }

        subQueries.addMetric(code).addTag("host", tag).addAggregator(aggregator).addDownsample(second + "s-" + aggregator);
        DateTime dateTime = DateTime.now();
        long now = dateTime.getMillis();
        long minutesAgo = dateTime.minusSeconds(second).getMillis();
        builder.getQuery().addStart(minutesAgo).addEnd(now).addSubQuery(subQueries);
        log.info("opentsdb query build info:", builder.build());
        try {
            SimpleHttpResponse response = client.pushQueries(builder, ExpectResponse.STATUS_CODE);
            String content = response.getContent();
            if (response.isSuccess()) {
                JSONArray jsonArray = JSON.parseArray(content);
                for (Object object : jsonArray) {
                    JSONObject json = (JSONObject) JSON.toJSON(object);
                    String dps = json.getString("dps");
                    Map<String, Object> map = JSON.parseObject(dps, Map.class);
                    return map;
                }
            }
        } catch (IOException e) {
            log.error("query opentsdb exception.", e);
        }
        return null;
    }

    /**
     * @param client
     * @param code
     * @param start
     * @param end
     * @param tag
     * @param func
     * @param downsample
     * @return
     * @throws IOException
     */
    public static Map<String, Object> queryOpenTSDB(HttpClientImpl client, String code, long start, long end, String tag, String func, String downsample) throws IOException {

        QueryBuilder builder = QueryBuilder.getInstance();
        SubQueries subQueries = new SubQueries();

        String aggregator = "";
        if (Aggregator.avg.toString().equals(func)) {
            aggregator = Aggregator.avg.toString();
        } else if (Aggregator.max.toString().equals(func)) {
            aggregator = Aggregator.max.toString();
        } else if (Aggregator.min.toString().equals(func)) {
            aggregator = Aggregator.min.toString();
        } else {
            aggregator = Aggregator.avg.toString();
        }

        subQueries.addMetric(code).addTag("host", tag).addAggregator(aggregator).addDownsample(downsample);
        builder.getQuery().addStart(start).addEnd(end).addSubQuery(subQueries);
        log.info("opentsdb query build info:", builder.build());
        try {
            SimpleHttpResponse response = client.pushQueries(builder, ExpectResponse.STATUS_CODE);
            String content = response.getContent();
            if (response.isSuccess()) {
                JSONArray jsonArray = JSON.parseArray(content);
                for (Object object : jsonArray) {
                    JSONObject json = (JSONObject) JSON.toJSON(object);
                    String dps = json.getString("dps");
                    Map<String, Object> map = JSON.parseObject(dps, Map.class);
                    return map;
                }
            }
        } catch (IOException e) {
            log.error("query opentsds exception.", e);
        }
        return null;
    }
}
