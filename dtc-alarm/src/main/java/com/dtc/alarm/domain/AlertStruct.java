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
public class AlertStruct {

    /**
     * 系统类型
     */

    private String systemSame;
    /**
     * ip
     */
    private String ip;

    /**
     * 前五位编码
     */
    private String zbFourName;

    /**
     * 最后一位小数点编码
     */
    private String zbLastCode;

    /**
     * 中文名
     */
    private String nameCN;

    /**
     * 英文名
     */
    private String nameEN;

    /**
     * event time
     */
    private String eventTime;

    /**
     * system_time
     */
    private String systemTime;

    /**
     * 具体告警值
     */
    private String value;

    /**
     * 告警等级
     */
    private String level;

    /**
     * 资产唯一值，asseti_id
     */
    private String uniqueId;

    /**
     * 阈值
     */
    private String threshold;

    /**
     * ip-code
     */
    private String gaojing;
}
