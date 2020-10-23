package com.dtc.java.analytic.V2.sink.opentsdb;

import com.dtc.java.analytic.V2.common.model.DataStruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.opentsdb.client.ExpectResponse;
import org.opentsdb.client.HttpClientImpl;
import org.opentsdb.client.builder.MetricBuilder;
import org.opentsdb.client.response.Response;

/**
 * Created on 2020-02-25
 *
 * @author :hao.li
 */
@Slf4j
public class PSinkToOpentsdb extends RichSinkFunction<DataStruct> {

    String properties;
    HttpClientImpl httpClient;

    public PSinkToOpentsdb(String prop) {
        this.properties = prop;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        httpClient = new HttpClientImpl(properties);
    }

    @Override
    public void invoke(DataStruct value) {

        try {
            long start = System.currentTimeMillis();
            //写入OpenTSDB
            MetricBuilder builder = MetricBuilder.getInstance();
            String metric = value.getZbFourName();
            String host = value.getHost();
            String id = value.getZbLastCode();
            long time = Long.parseLong(value.getTime());
            //转为保留两位小数
            java.text.DecimalFormat df = new java.text.DecimalFormat("#.##");
            double result = Double.parseDouble(df.format(Double.parseDouble(value.getValue())));
            if (value.getSystem_name().contains("h3c_switch")) {
                //华三交换机存储opentsdb策略
                if (id.isEmpty()) {
                    String metrics =metric + "-" +host;
                    builder.addMetric(metrics)
                            .setDataPoint(time, result)
                            .addTag("type", "h3c_swtich")
                            .addTag("host", host);
                } else {
                    String metrics = metric + "-" + host + "-" + id;
                    builder.addMetric(metrics)
                            .setDataPoint(time, result)
                            .addTag("type", "h3c_swtich")
                            .addTag("host", host)
                            .addTag("lastcode", id);
                }
            } else if (value.getSystem_name().contains("zx_swtich")) {
                //中兴交换机存储opentsdb策略
                if (id.isEmpty()) {
                    String metrics =metric + "-" +host;
                    builder.addMetric(metrics)
                            .setDataPoint(time, result)
                            .addTag("type", "zx_swtich")
                            .addTag("host", host);
                } else {
                    String metrics = metric + "-" + host + "-" + id;
                    builder.addMetric(metrics)
                            .setDataPoint(time, result)
                            .addTag("type", "zx_swtich")
                            .addTag("host", host)
                            .addTag("lastcode", id);
                }
            } else if (value.getSystem_name().contains("dpi")) {
                //dpi存储opentsdb策略
                String metrics = metric + "-" + host + "-" + id;
                builder.addMetric(metrics)
                        .setDataPoint(time, result)
                        .addTag("type", "dpi")
                        .addTag("host", host)
                        .addTag("lastcode", id);
            } else if (value.getSystem_name().contains("win")) {
                //windows存储opentsdb策略
                if (id.isEmpty()) {
                    String metrics = metric + "-" + host;
                    builder.addMetric(metrics)
                            .setDataPoint(time, result)
                            .addTag("type", "win")
                            .addTag("host", host);
                } else {
                    String metrics = metric + "-" + host + "-" + id;
                    builder.addMetric(metrics)
                            .setDataPoint(time, result)
                            .addTag("type", "win")
                            .addTag("host", host)
                            .addTag("lastcode", id);
                }
            } else if (value.getSystem_name().contains("aix")) {
                //aix存储opentsdb策略
                if (id.isEmpty()) {
                    String metrics = metric + "-" + host;
                    builder.addMetric(metrics)
                            .setDataPoint(time, result)
                            .addTag("type", "aix")
                            .addTag("host", host);
                } else {
                    String metrics = metric + "-" + host + "-" + id;
                    builder.addMetric(metrics)
                            .setDataPoint(time, result)
                            .addTag("type", "aix")
                            .addTag("lastcode", id)
                            .addTag("host", host);
                }
            } else if (value.getSystem_name().contains("h3c_router")) {
                //华三路由器存储opentsdb策略
                if (id.isEmpty()) {
                    String metrics = metric + "-" + host;
                    builder.addMetric(metrics)
                            .setDataPoint(time, result)
                            .addTag("type", "h3c_router")
                            .addTag("host", host);
                } else {
                    String metrics = metric + "-" + host + "-" + id;
                    builder.addMetric(metrics)
                            .setDataPoint(time, result)
                            .addTag("type", "h3c_router")
                            .addTag("lastcode", id)
                            .addTag("host", host);
                }
            } else {
                builder.addMetric(metric + "-" + host)
                        .setDataPoint(time, result)
                        .addTag("host", host);
            }
            Response response = httpClient.pushMetrics(builder, ExpectResponse.SUMMARY);
            if (response.isSuccess()) {
                log.info("sink data to opentsdb success,cost time {} ms", System.currentTimeMillis() - start);
            }
        } catch (Exception e) {
            log.error("sink data to opentsdb exception.", e);
        }
    }
}
