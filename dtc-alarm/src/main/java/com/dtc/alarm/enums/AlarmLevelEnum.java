package com.dtc.alarm.enums;

/**
 * @author
 */
public enum AlarmLevelEnum {
    /**
     * 告警等级
     */
    GENERAL("1", "一般"),
    LESS_SERIOUS("2", "较严重"),
    SERIOUS("3", "严重"),
    FATAL("4", "灾难");

    /**
     * 1：一般，2：较严重，3：严重，4：灾难
     */
    private String levelCode;

    /**
     * 告警描述
     */
    private String desc;

    AlarmLevelEnum(String levelCode, String desc) {
        this.levelCode = levelCode;
        this.desc = desc;
    }

    public String getLevelCode() {
        return levelCode;
    }

    public AlarmLevelEnum setLevelCode(String levelCode) {
        this.levelCode = levelCode;
        return this;
    }

    public String getDesc() {
        return desc;
    }

    public AlarmLevelEnum setDesc(String desc) {
        this.desc = desc;
        return this;
    }
}
