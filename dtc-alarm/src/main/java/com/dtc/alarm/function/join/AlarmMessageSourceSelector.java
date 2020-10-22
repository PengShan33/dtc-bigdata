package com.dtc.alarm.function.join;

import com.dtc.alarm.domain.AlarmMessage;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author
 */
public class AlarmMessageSourceSelector implements KeySelector<AlarmMessage, Tuple2<String, String>> {

    @Override
    public Tuple2<String, String> getKey(AlarmMessage alarmMessage) throws Exception {

        return new Tuple2<String, String>(alarmMessage.getCode(), alarmMessage.getIpv4());
    }
}
