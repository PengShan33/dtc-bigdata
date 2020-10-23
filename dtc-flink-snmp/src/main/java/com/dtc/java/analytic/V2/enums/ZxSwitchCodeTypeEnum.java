package com.dtc.java.analytic.V2.enums;

public enum ZxSwitchCodeTypeEnum {
    /**
     * 指标编码
     */
    CPU_PERCET("102_103_101_101_101", "cpu利用率"),
    MEM_SIZE("102_103_101_102_102","内存总量"),
    MEM_USED_PERCET("102_103_102_103_103", "内存利用率"),
    PORT_SPEED_OUT("102_103_103_105_105","端口出速率"),
    PORT_SPEED_IN("102_103_103_106_106","端口入速率"),
    IF_INERRORS("102_103_103_108_108", "端口入方向错误报文数"),
    IF_OUTERRORS("102_103_103_110_110", "端口出方向错误报文数"),
    PORT_STATUS("102_103_103_111_111", "端口工作状态"),
    IF_OUTOCTETS("102_103_103_109_109", "端口出方向字节数"),
    IF_INOCTETS("102_103_103_107_107", "端口入方向字节数");

    /**
     * 采集编码
     */
    private String code;

    /**
     * 描述
     */
    private String desc;

    ZxSwitchCodeTypeEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public String getCode() {
        return code;
    }

    public ZxSwitchCodeTypeEnum setCode(String code) {
        this.code = code;
        return this;
    }

    public String getDesc() {
        return desc;
    }

    public ZxSwitchCodeTypeEnum setDesc(String desc) {
        this.desc = desc;
        return this;
    }
}
