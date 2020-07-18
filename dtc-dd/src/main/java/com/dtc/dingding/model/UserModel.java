package com.dtc.dingding.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserModel extends SuperModel {
    String unionid;
    String openId;
    String mobile;
    String department;
    String OnDuty;
    String OffDuty;
    String name;
    String OnResult;
    String OffResult;

}
