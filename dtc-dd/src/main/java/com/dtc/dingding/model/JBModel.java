package com.dtc.dingding.model;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class JBModel extends SuperModel{
    @SerializedName(value="jiaban_time")
    String time;
    @SerializedName("start_time")
    String starttime;
    @SerializedName("end_time")
    String endtime;
    @SerializedName("reason")
    String reason;
    @SerializedName("approve_status")
    String status;
    @SerializedName("first_approver_userid")
    String firstApprover;
    @SerializedName("first_approve_time")
    String firstApproveTime;
    @SerializedName("second_approver_userid")
    String secondApprover;
    @SerializedName("second_approve_time")
    String secondApproveTime;
}
