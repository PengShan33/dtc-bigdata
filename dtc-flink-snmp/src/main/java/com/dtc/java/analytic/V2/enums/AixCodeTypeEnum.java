package com.dtc.java.analytic.V2.enums;

/**
 * @author
 */
public enum AixCodeTypeEnum {

    /**
     * 指标编码
     */
    AIX_SYSTEM_UP_TIME("101_102_101_108_108", "系统启动时间"),
    AIX_SECPU_UTILIZATION("101_102_102_103_103", "cpu使用率"),
    AIX_MEMORY_SIZE("101_102_103_101_101", "内存大小"),
    AIX_STORAGE_ALLOCATION_UNITS("101_102_104_104_104", "簇的大小"),
    AIX_STORAGE_SIZE("101_102_104_105_105", "簇的数目"),
    AIX_STORAGE_USED("101_102_104_106_106", "每个分区使用过的簇的个数"),
    AIX_FS_SIZE("101_102_104_108_108", "磁盘大小"),
    AIX_FS_FREE("101_102_104_109_109", "磁盘剩余大小"),
    AIX_IN_UCAST_PKTS("101_102_105_110_110", "通过上层协议传递到子网的单播报文数"),
    AIX_IN_NUCAST_PKTS("101_102_105_112_112", "传递给上层网络协议的非单报文数"),
    AIX_OUT_UCAST_PKTS("101_102_105_111_111", "上层协议（如IP）需要发送给一个网络单播地址的报文数"),
    AIX_OUT_NUCAST_PKTS("101_102_105_114_114", "上层协议（如IP）需要发送给一个非单播地址的报文数"),
    AIX_OUT_ERRORS("101_102_105_115_115", "由于错误而不能发送的报文数量"),
    AIX_IN_ERRORS("101_102_105_113_113", "流入的错误报文数"),
    AIX_IN_OCTETS("101_102_105_101_101", "端口入流量"),
    AIX_OUT_OCTETS("101_102_105_102_102", "端口出流量");


    /**
     * 采集编码
     */
    private String code;

    /**
     * 描述
     */
    private String desc;

    AixCodeTypeEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public String getCode() {
        return code;
    }

    public AixCodeTypeEnum setCode(String code) {
        this.code = code;
        return this;
    }

    public String getDesc() {
        return desc;
    }

    public AixCodeTypeEnum setDesc(String desc) {
        this.desc = desc;
        return this;
    }
}
