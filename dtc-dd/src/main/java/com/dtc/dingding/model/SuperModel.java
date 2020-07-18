package com.dtc.dingding.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public abstract class SuperModel {
    String userid;

    public String getUserid() {
        return userid;
    }
}
