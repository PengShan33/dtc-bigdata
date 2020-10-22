package com.dtc.alarm.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class AlarmMessage {

    /**
     * 资产id
     */
    private String assetId;

    /**
     * 告警触发条件及资产id
     */
    private String conAssetId;

    /**
     * 告警触发条件
     */
    private String conAlarm;

    /**
     * 策略id
     */
    private String strategyId;

    /**
     * ip地址
     */
    private String ipv4;

    /**
     * 策略类型
     */
    private String triggerKind;

    /**
     * 策略名称
     */
    private String triggerName;

    /**
     * 运算符
     */
    private String comparator;

    /**
     * 阈值
     */
    private Double number;

    /**
     * 单位
     */
    private String unit;

    /**
     * 指标编码
     */
    private String code;

    /**
     * 告警等级
     */
    private String alarmLevel;

    /**
     * 策略更新时间
     */
    private String upTime;

    /**
     * 资产编码
     */
    private String assetCode;

    /**
     * 资产名称
     */
    private String name;

    /**
     * 时间(单位：秒)
     */
    private Integer pastTimeSecond;

    /**
     * 1:最大值 2:最小值 3:平均值
     */
    private Integer target;

}
