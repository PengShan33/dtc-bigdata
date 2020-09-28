package com.dtc.java.analytic.V2.common.model;

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
public class AlarmDto {

    private String code;
    private String name;
    private String assetId;
    private String realValue;
    private String alarmGrade;
    private String description;
    private String eventTime;
    private String rule;
    private String indexId;
    private String strategyId;

}
