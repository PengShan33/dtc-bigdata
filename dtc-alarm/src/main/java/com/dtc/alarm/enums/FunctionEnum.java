package com.dtc.alarm.enums;

/**
 * @author
 */
public enum FunctionEnum {

    /**
     * 1:最大值 2:最小值 3:平均值
     */
    MAX(1, "最大值"),
    MIN(2, "最小值"),
    AVG(3, "平均值");

    private Integer order;
    private String desc;

    FunctionEnum(Integer order, String desc) {
        this.order = order;
        this.desc = desc;
    }

    public Integer getOrder() {
        return order;
    }

    public FunctionEnum setOrder(Integer order) {
        this.order = order;
        return this;
    }

    public String getDesc() {
        return desc;
    }

    public FunctionEnum setDesc(String desc) {
        this.desc = desc;
        return this;
    }
}
