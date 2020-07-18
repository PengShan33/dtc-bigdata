package com.dtc.dingding.model;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BKModel extends SuperModel{
    @SerializedName(value="buka_time")
    String time;
}