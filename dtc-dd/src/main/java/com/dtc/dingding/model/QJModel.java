package com.dtc.dingding.model;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class QJModel extends SuperModel{
    @SerializedName(value="qingjia_time")
    String time;
    @SerializedName("start_time")
    String starttime;
    @SerializedName("end_time")
    String endtime;

}
