package com.dtc.java.SC.JKZL.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author : lihao
 * Created on : 2020-03-27
 * @Description : TODO描述类作用
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class YCSB_LB_Model {
    private String asset_id;
    private String level_id;
    private int num;
    private String name;
    private String ip;
    private String room;
    private String partitions;
    private String box;
}
