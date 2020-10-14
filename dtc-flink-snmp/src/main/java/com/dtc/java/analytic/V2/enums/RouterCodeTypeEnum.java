package com.dtc.java.analytic.V2.enums;

/**
 * @author
 */
public enum RouterCodeTypeEnum {

    /**
     * 路由器指标编码
     */
    ROUTER_OPER_STATUS("103_101_101_101_101", "端口运行状态"),
    ROUTER_IN_ERRORS("103_101_101_102_102", "获取端口入方向错包数"),
    ROUTER_OUT_ERRORS("103_101_101_103_103", "获取端口出方向错包数"),
    ROUTER_HCIN_OCTETS("103_101_101_104_104", "获取端口入方向字节数"),
    ROUTER_HCOUT_OCTETS("103_101_101_105_105", "获取端口出方向字节数"),
    ROUTER_CPU_USAGE("103_102_101_101_101", "单板CPU利用率"),
    ROUTER_SYS_CPURATIO("103_102_101_102_102", "主用主控板CPU利用率"),
    ROUTER_SLOT_CPURATIO("103_102_101_103_103", "所有单板CPU利用率"),
    ROUTER_MEM_USAGE("103_103_101_101_101", "单板内存利用率"),
    ROUTER_SYS_MEMRATIO("103_103_101_102_102", "主用主控板内存利用率"),
    ROUTER_SLOT_MEMRATIO("103_103_101_103_103", "所有单板内存利用率");

    /**
     * 采集编码
     */
    private String code;

    /**
     * 描述
     */
    private String desc;

    RouterCodeTypeEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public String getCode() {
        return code;
    }

    public RouterCodeTypeEnum setCode(String code) {
        this.code = code;
        return this;
    }

    public String getDesc() {
        return desc;
    }

    public RouterCodeTypeEnum setDesc(String desc) {
        this.desc = desc;
        return this;
    }
}
