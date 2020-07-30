package com.dtc.java.analytic.V2.map.function;

import com.dtc.java.analytic.V2.common.model.DataStruct;
import org.apache.flink.api.common.functions.MapFunction;

public class AixMapFunction implements MapFunction<DataStruct, DataStruct> {
    @Override
    public DataStruct map(DataStruct event) throws Exception {
        String zbLastCode = event.getZbLastCode();
        if (zbLastCode.contains(".")) {
            String lastCode = zbLastCode.split("\\.", 2)[0];
            String zbLastCode_Result = zbLastCode.split("\\.", 2)[1];
            String code_Result = event.getZbFourName() + "_" +  lastCode;
            return new DataStruct(event.getSystem_name(), event.getHost(), code_Result, zbLastCode_Result, event.getNameCN(), event.getNameEN(), event.getTime(), event.getValue());
        } else {
            String zbLastCode_Result = "";
            String code_Result = event.getZbFourName() + "_" + event.getZbLastCode();
            return new DataStruct(event.getSystem_name(), event.getHost(), code_Result, zbLastCode_Result, event.getNameCN(), event.getNameEN(), event.getTime(), event.getValue());
        }
    }
}
