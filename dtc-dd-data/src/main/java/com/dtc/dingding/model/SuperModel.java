package com.dtc.dingding.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created on 2020-03-09
 *
 * @author :hao.li
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public abstract class SuperModel {
    String userid;

    public String getUserid() {
        return userid;
    }
}
