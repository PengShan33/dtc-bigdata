package com.dtc.alarm.util;

import com.dtc.alarm.domain.AlterStruct;
import org.apache.flink.util.Collector;

public class RuleCompareUtil {

    public static void bigger(double result, String levels, String rule, Collector<AlterStruct> out) {

        String[] split = levels.split("\\|");
        String level_1 = split[0];
        String level_2 = split[1];
        String level_3 = split[2];
        String level_4 = split[3];
        String level = null;
        Double threshold = null;

        //四个阈值都不为空
        if (!("null".equals(level_1)) && !("null".equals(level_2)) && !("null".equals(level_3)) && !("null".equals(level_4))) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_2 = Double.parseDouble(split[1]);
            Double num_3 = Double.parseDouble(split[2]);
            Double num_4 = Double.parseDouble(split[3]);
            if (result > num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result > num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result > num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result > num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        }
        //一个不为空，其他的都为空
        else if (!("null".equals(level_1)) && "null".equals(level_2) && "null".equals(level_3) && "null".equals(level_4)) {
            Double num_1 = Double.parseDouble(split[0]);
            if ((result > num_1)) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if ("null".equals(level_1) && !("null".equals(level_2)) && "null".equals(level_3) && "null".equals(level_4)) {
            Double num_2 = Double.parseDouble(split[1]);
            if (result > num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if ("null".equals(level_1) && "null".equals(level_2) && !("null".equals(level_3)) && "null".equals(level_4)) {
            Double num_3 = Double.parseDouble(split[2]);
            if (result > num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if ("null".equals(level_1) && "null".equals(level_2) && "null".equals(level_3) && !("null".equals(level_4))) {
            Double num_4 = Double.parseDouble(split[3]);
            if (result > num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        }
        //两个为空，两个不为空
        else if (!("null".equals(level_1)) && !("null".equals(level_2)) && "null".equals(level_3) && "null".equals(level_4)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_2 = Double.parseDouble(split[1]);
            if (result > num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result > num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_1)) && !("null".equals(level_3)) && "null".equals(level_2) && "null".equals(level_4)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_3 = Double.parseDouble(split[2]);
            if (result > num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result > num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_1)) && !("null".equals(level_4)) && "null".equals(level_2) && "null".equals(level_3)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_4 = Double.parseDouble(split[3]);
            if (result > num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result > num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_2)) && !("null".equals(level_3)) && "null".equals(level_1) && "null".equals(level_4)) {
            Double num_3 = Double.parseDouble(split[2]);
            Double num_2 = Double.parseDouble(split[1]);
            if (result > num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result > num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_2)) && !("null".equals(level_4)) && "null".equals(level_1) && "null".equals(level_3)) {
            Double num_4 = Double.parseDouble(split[3]);
            Double num_2 = Double.parseDouble(split[1]);
            if (result > num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result > num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_3)) && !("null".equals(level_4)) && "null".equals(level_1) && "null".equals(level_2)) {
            Double num_4 = Double.parseDouble(split[3]);
            Double num_3 = Double.parseDouble(split[2]);
            if (result > num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result > num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        }
        //三不空，一空
        else if (!("null".equals(level_1)) && !("null".equals(level_2)) && !("null".equals(level_3)) && "null".equals(level_4)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_2 = Double.parseDouble(split[1]);
            Double num_3 = Double.parseDouble(split[2]);
            if (result > num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result > num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result > num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_1)) && !("null".equals(level_2)) && !("null".equals(level_4)) && "null".equals(level_3)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_2 = Double.parseDouble(split[1]);
            Double num_4 = Double.parseDouble(split[3]);
            if (result > num_1 ) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result > num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result > num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_1)) && !("null".equals(level_3)) && !("null".equals(level_4)) && "null".equals(level_2)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_3 = Double.parseDouble(split[2]);
            Double num_4 = Double.parseDouble(split[3]);
            if (result > num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result > num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result > num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_2)) && !("null".equals(level_3)) && !("null".equals(level_4)) && "null".equals(level_1)) {
            Double num_2 = Double.parseDouble(split[1]);
            Double num_3 = Double.parseDouble(split[2]);
            Double num_4 = Double.parseDouble(split[3]);
            if (result > num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result > num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result > num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        }
    }

    public static void lower(double result, String levels, String rule, Collector<AlterStruct> out) {

        String[] split = levels.split("\\|");
        String level_1 = split[0];
        String level_2 = split[1];
        String level_3 = split[2];
        String level_4 = split[3];
        String level = null;
        Double threshold = null;

        //四个阈值都不为空
        if (!("null".equals(level_1)) && !("null".equals(level_2)) && !("null".equals(level_3)) && !("null".equals(level_4))) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_2 = Double.parseDouble(split[1]);
            Double num_3 = Double.parseDouble(split[2]);
            Double num_4 = Double.parseDouble(split[3]);
            if (result < num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result < num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result < num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result < num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        }
        //一个不为空，其他的都为空
        else if (!("null".equals(level_1)) && "null".equals(level_2) && "null".equals(level_3) && "null".equals(level_4)) {
            Double num_1 = Double.parseDouble(split[0]);
            if ((result < num_1)) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if ("null".equals(level_1) && !("null".equals(level_2)) && "null".equals(level_3) && "null".equals(level_4)) {
            Double num_2 = Double.parseDouble(split[1]);
            if (result < num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if ("null".equals(level_1) && "null".equals(level_2) && !("null".equals(level_3)) && "null".equals(level_4)) {
            Double num_3 = Double.parseDouble(split[2]);
            if (result < num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if ("null".equals(level_1) && "null".equals(level_2) && "null".equals(level_3) && !("null".equals(level_4))) {
            Double num_4 = Double.parseDouble(split[3]);
            if (result < num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        }
        //两个为空，两个不为空
        else if (!("null".equals(level_1)) && !("null".equals(level_2)) && "null".equals(level_3) && "null".equals(level_4)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_2 = Double.parseDouble(split[1]);
            if (result < num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result < num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_1)) && !("null".equals(level_3)) && "null".equals(level_2) && "null".equals(level_4)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_3 = Double.parseDouble(split[2]);
            if (result < num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result < num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_1)) && !("null".equals(level_4)) && "null".equals(level_2) && "null".equals(level_3)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_4 = Double.parseDouble(split[3]);
            if (result < num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result < num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_2)) && !("null".equals(level_3)) && "null".equals(level_1) && "null".equals(level_4)) {
            Double num_3 = Double.parseDouble(split[2]);
            Double num_2 = Double.parseDouble(split[1]);
            if (result < num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result < num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_2)) && !("null".equals(level_4)) && "null".equals(level_1) && "null".equals(level_3)) {
            Double num_4 = Double.parseDouble(split[3]);
            Double num_2 = Double.parseDouble(split[1]);
            if (result < num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result < num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_3)) && !("null".equals(level_4)) && "null".equals(level_1) && "null".equals(level_2)) {
            Double num_4 = Double.parseDouble(split[3]);
            Double num_3 = Double.parseDouble(split[2]);
            if (result < num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result < num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        }
        //三不空，一空
        else if (!("null".equals(level_1)) && !("null".equals(level_2)) && !("null".equals(level_3)) && "null".equals(level_4)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_2 = Double.parseDouble(split[1]);
            Double num_3 = Double.parseDouble(split[2]);
            if (result < num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result < num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result < num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_1)) && !("null".equals(level_2)) && !("null".equals(level_4)) && "null".equals(level_3)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_2 = Double.parseDouble(split[1]);
            Double num_4 = Double.parseDouble(split[3]);
            if (result < num_1 ) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result < num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result < num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_1)) && !("null".equals(level_3)) && !("null".equals(level_4)) && "null".equals(level_2)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_3 = Double.parseDouble(split[2]);
            Double num_4 = Double.parseDouble(split[3]);
            if (result < num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result < num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result < num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_2)) && !("null".equals(level_3)) && !("null".equals(level_4)) && "null".equals(level_1)) {
            Double num_2 = Double.parseDouble(split[1]);
            Double num_3 = Double.parseDouble(split[2]);
            Double num_4 = Double.parseDouble(split[3]);
            if (result < num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result < num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result < num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        }
    }

    public static void equal(double result, String levels, String rule, Collector<AlterStruct> out) {

        String[] split = levels.split("\\|");
        String level_1 = split[0];
        String level_2 = split[1];
        String level_3 = split[2];
        String level_4 = split[3];
        String level = null;
        Double threshold = null;

        //四个阈值都不为空
        if (!("null".equals(level_1)) && !("null".equals(level_2)) && !("null".equals(level_3)) && !("null".equals(level_4))) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_2 = Double.parseDouble(split[1]);
            Double num_3 = Double.parseDouble(split[2]);
            Double num_4 = Double.parseDouble(split[3]);
            if (result == num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result == num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result == num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result == num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        }
        //一个不为空，其他的都为空
        else if (!("null".equals(level_1)) && "null".equals(level_2) && "null".equals(level_3) && "null".equals(level_4)) {
            Double num_1 = Double.parseDouble(split[0]);
            if ((result == num_1)) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if ("null".equals(level_1) && !("null".equals(level_2)) && "null".equals(level_3) && "null".equals(level_4)) {
            Double num_2 = Double.parseDouble(split[1]);
            if (result == num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if ("null".equals(level_1) && "null".equals(level_2) && !("null".equals(level_3)) && "null".equals(level_4)) {
            Double num_3 = Double.parseDouble(split[2]);
            if (result == num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if ("null".equals(level_1) && "null".equals(level_2) && "null".equals(level_3) && !("null".equals(level_4))) {
            Double num_4 = Double.parseDouble(split[3]);
            if (result == num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        }
        //两个为空，两个不为空
        else if (!("null".equals(level_1)) && !("null".equals(level_2)) && "null".equals(level_3) && "null".equals(level_4)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_2 = Double.parseDouble(split[1]);
            if (result == num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result == num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_1)) && !("null".equals(level_3)) && "null".equals(level_2) && "null".equals(level_4)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_3 = Double.parseDouble(split[2]);
            if (result == num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result == num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_1)) && !("null".equals(level_4)) && "null".equals(level_2) && "null".equals(level_3)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_4 = Double.parseDouble(split[3]);
            if (result == num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result == num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_2)) && !("null".equals(level_3)) && "null".equals(level_1) && "null".equals(level_4)) {
            Double num_3 = Double.parseDouble(split[2]);
            Double num_2 = Double.parseDouble(split[1]);
            if (result == num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result == num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_2)) && !("null".equals(level_4)) && "null".equals(level_1) && "null".equals(level_3)) {
            Double num_4 = Double.parseDouble(split[3]);
            Double num_2 = Double.parseDouble(split[1]);
            if (result == num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result == num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_3)) && !("null".equals(level_4)) && "null".equals(level_1) && "null".equals(level_2)) {
            Double num_4 = Double.parseDouble(split[3]);
            Double num_3 = Double.parseDouble(split[2]);
            if (result == num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result == num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        }
        //三不空，一空
        else if (!("null".equals(level_1)) && !("null".equals(level_2)) && !("null".equals(level_3)) && "null".equals(level_4)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_2 = Double.parseDouble(split[1]);
            Double num_3 = Double.parseDouble(split[2]);
            if (result == num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result == num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result == num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_1)) && !("null".equals(level_2)) && !("null".equals(level_4)) && "null".equals(level_3)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_2 = Double.parseDouble(split[1]);
            Double num_4 = Double.parseDouble(split[3]);
            if (result == num_1 ) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result == num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result == num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_1)) && !("null".equals(level_3)) && !("null".equals(level_4)) && "null".equals(level_2)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_3 = Double.parseDouble(split[2]);
            Double num_4 = Double.parseDouble(split[3]);
            if (result == num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result == num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result == num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_2)) && !("null".equals(level_3)) && !("null".equals(level_4)) && "null".equals(level_1)) {
            Double num_2 = Double.parseDouble(split[1]);
            Double num_3 = Double.parseDouble(split[2]);
            Double num_4 = Double.parseDouble(split[3]);
            if (result == num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result == num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result == num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        }
    }

    public static void unequal(double result, String levels, String rule, Collector<AlterStruct> out) {

        String[] split = levels.split("\\|");
        String level_1 = split[0];
        String level_2 = split[1];
        String level_3 = split[2];
        String level_4 = split[3];
        String level = null;
        Double threshold = null;

        //四个阈值都不为空
        if (!("null".equals(level_1)) && !("null".equals(level_2)) && !("null".equals(level_3)) && !("null".equals(level_4))) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_2 = Double.parseDouble(split[1]);
            Double num_3 = Double.parseDouble(split[2]);
            Double num_4 = Double.parseDouble(split[3]);
            if (result != num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result != num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result != num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result != num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        }
        //一个不为空，其他的都为空
        else if (!("null".equals(level_1)) && "null".equals(level_2) && "null".equals(level_3) && "null".equals(level_4)) {
            Double num_1 = Double.parseDouble(split[0]);
            if ((result != num_1)) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if ("null".equals(level_1) && !("null".equals(level_2)) && "null".equals(level_3) && "null".equals(level_4)) {
            Double num_2 = Double.parseDouble(split[1]);
            if (result != num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if ("null".equals(level_1) && "null".equals(level_2) && !("null".equals(level_3)) && "null".equals(level_4)) {
            Double num_3 = Double.parseDouble(split[2]);
            if (result != num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if ("null".equals(level_1) && "null".equals(level_2) && "null".equals(level_3) && !("null".equals(level_4))) {
            Double num_4 = Double.parseDouble(split[3]);
            if (result != num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        }
        //两个为空，两个不为空
        else if (!("null".equals(level_1)) && !("null".equals(level_2)) && "null".equals(level_3) && "null".equals(level_4)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_2 = Double.parseDouble(split[1]);
            if (result != num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result != num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_1)) && !("null".equals(level_3)) && "null".equals(level_2) && "null".equals(level_4)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_3 = Double.parseDouble(split[2]);
            if (result != num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result != num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_1)) && !("null".equals(level_4)) && "null".equals(level_2) && "null".equals(level_3)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_4 = Double.parseDouble(split[3]);
            if (result != num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result != num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_2)) && !("null".equals(level_3)) && "null".equals(level_1) && "null".equals(level_4)) {
            Double num_3 = Double.parseDouble(split[2]);
            Double num_2 = Double.parseDouble(split[1]);
            if (result != num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result != num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_2)) && !("null".equals(level_4)) && "null".equals(level_1) && "null".equals(level_3)) {
            Double num_4 = Double.parseDouble(split[3]);
            Double num_2 = Double.parseDouble(split[1]);
            if (result != num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result != num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_3)) && !("null".equals(level_4)) && "null".equals(level_1) && "null".equals(level_2)) {
            Double num_4 = Double.parseDouble(split[3]);
            Double num_3 = Double.parseDouble(split[2]);
            if (result != num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result != num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        }
        //三不空，一空
        else if (!("null".equals(level_1)) && !("null".equals(level_2)) && !("null".equals(level_3)) && "null".equals(level_4)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_2 = Double.parseDouble(split[1]);
            Double num_3 = Double.parseDouble(split[2]);
            if (result != num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result != num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result != num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_1)) && !("null".equals(level_2)) && !("null".equals(level_4)) && "null".equals(level_3)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_2 = Double.parseDouble(split[1]);
            Double num_4 = Double.parseDouble(split[3]);
            if (result != num_1 ) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result != num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result != num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_1)) && !("null".equals(level_3)) && !("null".equals(level_4)) && "null".equals(level_2)) {
            Double num_1 = Double.parseDouble(split[0]);
            Double num_3 = Double.parseDouble(split[2]);
            Double num_4 = Double.parseDouble(split[3]);
            if (result != num_1) {
                level = "1";
                threshold = num_1;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result != num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result != num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        } else if (!("null".equals(level_2)) && !("null".equals(level_3)) && !("null".equals(level_4)) && "null".equals(level_1)) {
            Double num_2 = Double.parseDouble(split[1]);
            Double num_3 = Double.parseDouble(split[2]);
            Double num_4 = Double.parseDouble(split[3]);
            if (result != num_2) {
                level = "2";
                threshold = num_2;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result != num_3) {
                level = "3";
                threshold = num_3;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
            if (result != num_4) {
                level = "4";
                threshold = num_4;
                AlterStruct alter_message = getAlterMassage(result, level, threshold, rule);
                out.collect(alter_message);
            }
        }
    }

    public static AlterStruct getAlterMassage(double result, String level, Double threshold, String rule) {
        String system_time = String.valueOf(System.currentTimeMillis());
        String realData = String.valueOf(result);
        String thresholdData = String.valueOf(threshold);

        String[] split = rule.split(":");
        String assetId = split[0];
        String code = split[1];
        String assetCode = split[2];
        String assetName = split[3];
        String ip = split[4];
        String conAssetId = split[5];
        String strategyId = split[6];
        String triggerKind = split[7];
        String triggerName = split[8];
        String comparator = split[9];
        String unit = split[10];
        String upTime = split[11];
        String pastTimeSecond = split[12];
        String target = split[13];
        String conAlarm = split[14];
        String pastTime = split[15];
        String timeUnit = split[16];
        String gaojing = ip + "-" + code;


        String targetDescription = null;
        if ("1".equals(target)) {
            targetDescription = "最大值";
        }
        if ("2".equals(target)) {
            targetDescription = "最小值";
        }
        if ("3".equals(target)) {
            targetDescription = "平均值";
        }
        String description = assetName + "(" + assetCode + ")的" + triggerName + "在过去的" + pastTime + timeUnit + "的" + targetDescription + "是" + realData + unit + ",而阀值是" + thresholdData + unit;
        String ruleDescription = "当采集的" + triggerName + "在过去的" + pastTime + timeUnit + "的" + targetDescription + comparator + thresholdData + unit + "时,触发告警";

        AlterStruct alterMessage = new AlterStruct();
        alterMessage.setIp(ip);
        alterMessage.setZbFourName(code);
        alterMessage.setTriggerName(triggerName);
        alterMessage.setOccurTime(system_time);
        alterMessage.setThreshold(thresholdData);
        alterMessage.setValue(realData);
        alterMessage.setLevel(level);
        alterMessage.setDescription(description);
        alterMessage.setRule(ruleDescription);
        alterMessage.setUniqueId(assetId);
        alterMessage.setGaojing(gaojing);
        alterMessage.setCon_alarm(conAlarm);
        alterMessage.setCon_asset_id(conAssetId);

        return alterMessage;
    }
}
