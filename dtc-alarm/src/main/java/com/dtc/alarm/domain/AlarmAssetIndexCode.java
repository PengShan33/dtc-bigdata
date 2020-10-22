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
public class AlarmAssetIndexCode {

    /**
     * 指标编码
     */
    private String code;

    /**
     * IP地址
     */
    private String ip;

    /**
     * 指标子编码(端口/板卡编码)
     */
    private String subCode;
}
