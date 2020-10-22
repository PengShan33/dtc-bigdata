package com.dtc.alarm.function.join;

import com.dtc.alarm.domain.AlarmAssetIndexCode;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author
 */
public class IndexCodeSourceSelector implements KeySelector<AlarmAssetIndexCode, Tuple2<String, String>> {

    @Override
    public Tuple2<String, String> getKey(AlarmAssetIndexCode indexCode) throws Exception {

        return new Tuple2<String, String>(indexCode.getCode(), indexCode.getIp());
    }
}
