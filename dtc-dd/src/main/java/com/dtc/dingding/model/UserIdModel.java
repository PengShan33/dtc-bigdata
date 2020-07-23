package com.dtc.dingding.model;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserIdModel extends SuperModel{
    @SerializedName("name")
    String name;
    @SerializedName("mobile")
    String mobile;
}
