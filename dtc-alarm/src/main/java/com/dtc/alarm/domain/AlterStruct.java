package com.dtc.alarm.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AlterStruct {
    /**
     * ip
     */
    private String ip;

    /**
     * 前五位编码code
     */
    private String zbFourName;

    /**
     * 指标名称
     */
    private String triggerName;

    /**
     * 告警触发时间
     */
    private String occurTime;

    /**
     * 阈值
     */
    private String threshold;

    /**
     * 具体告警值
     */
    private String value;

    /**
     * 告警等级
     */
    private String level;

    /**
     * 告警描述
     */
    private String description;

    /**
     * 告警规则
     */
    private String rule;

    /**
     * 资产唯一值，asset_id
     */
    private String uniqueId;

    /**
     * ip-code
     */
    private String gaojing;

    /**
     *资产id+告警触发条件
     */
    private String con_alarm;

    /**
     * 告警触发条件
     */
    private String con_asset_id;
}
