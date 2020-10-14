package com.dtc.java.analytic.V2.map.function;

import com.dtc.java.analytic.V2.common.model.DataStruct;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author
 */
public class RouterMapFunction implements MapFunction<DataStruct, DataStruct> {

    @Override
    public DataStruct map(DataStruct event) throws Exception {
        String zbLastCode = event.getZbLastCode();
        if (zbLastCode.contains(".")) {
            String lastCode = zbLastCode.split("\\.", 2)[0];
            String zbLastCodeResult = zbLastCode.split("\\.", 2)[1];
            String codeResult = event.getZbFourName() + "_" + lastCode;
            return new DataStruct(event.getSystem_name() + "|router_0", event.getHost(), codeResult, zbLastCodeResult, event.getNameCN(), event.getNameEN(), event.getTime(), event.getValue());
        } else {
            String zbLastCodeResult = "";
            String codeResult = event.getZbFourName() + "_" + event.getZbLastCode();
            return new DataStruct(event.getSystem_name() + "|router_1", event.getHost(), codeResult, zbLastCodeResult, event.getNameCN(), event.getNameEN(), event.getTime(), event.getValue());
        }
    }
}
