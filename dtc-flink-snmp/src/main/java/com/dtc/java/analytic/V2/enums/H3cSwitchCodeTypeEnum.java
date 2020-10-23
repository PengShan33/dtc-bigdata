package com.dtc.java.analytic.V2.enums;

public enum H3cSwitchCodeTypeEnum {
    /**
     * 指标编码
     */
    CPU_PERCET("102_101_101_101_101", "cpu利用率"),
    MEM_USED_PERCET("102_101_102_103_103", "内存利用率"),
    IF_INERRORS("102_101_103_107_108", "端口入方向错误报文数"),
    IF_OUTERRORS("102_101_103_109_110", "端口出方向错误报文数"),
    PORT_STATUS("102_101_103_110_111", "端口工作状态"),
    IF_OUTOCTETS("102_101_103_108_109", "端口出方向字节数"),
    IF_INOCTETS("102_101_103_106_107", "端口入方向字节数");

    /**
     * 采集编码
     */
    private String code;

    /**
     * 描述
     */
    private String desc;

    H3cSwitchCodeTypeEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public String getCode() {
        return code;
    }

    public H3cSwitchCodeTypeEnum setCode(String code) {
        this.code = code;
        return this;
    }

    public String getDesc() {
        return desc;
    }

    public H3cSwitchCodeTypeEnum setDesc(String desc) {
        this.desc = desc;
        return this;
    }
}
