package com.dtc.java.analytic.V2.sink.redis;

import com.dtc.java.analytic.V2.common.model.AlarmDto;
import com.dtc.java.analytic.V2.common.model.AlterStruct;
import com.dtc.java.analytic.V2.common.utils.KyroSerialUtil;
import com.dtc.java.analytic.V2.common.utils.UUIDGenerator;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author
 */
public class RedisWriter extends RichSinkFunction<AlterStruct> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisWriter.class);

    private Jedis jedis;
    private ParameterTool parameterTool;

    /**
     * 放入reids中的DB的位置
     */
    private static final Integer REDIS_INDEX_FILE_COUNT = 10;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        jedis = RedisUtil.getResource(parameterTool);
        jedis.select(REDIS_INDEX_FILE_COUNT);
    }

    @Override
    public void invoke(AlterStruct value, Context context) throws Exception {

        String code = UUIDGenerator.generateUserCode();
        String codeName = value.getSystem_name();
        String nameCN = value.getNameCN();
        String UniqueId = value.getUnique_id();
        String assetId = null;
        String indexId = null;
        String strategyId = null;
        if (UniqueId.contains("|")) {
            String[] split = UniqueId.split("\\|");
            assetId = split[0];
            indexId = split[1];
            strategyId = split[2];
        }
        String realValue = value.getValue();
        String alarmGrade = value.getLevel();
        String description = codeName + "的" + nameCN + "是" + realValue;
        String alarmThreshold = value.getYuzhi();
        String rule = nameCN + "是" + realValue + ",而阈值是:" + alarmThreshold;
        String eventTime = timeStamp2Date(value.getEvent_time(), "yyyy-MM-dd HH:mm:ss");

        AlarmDto alarmDto = AlarmDto.builder().code(code).name(nameCN).assetId(assetId).
                realValue(realValue).alarmGrade(alarmGrade).description(description)
                .eventTime(eventTime).rule(rule).indexId(indexId)
                .strategyId(strategyId).build();

        String newJsonData = new Gson().toJson(alarmDto);
        String key = timeStamp2Date(value.getEvent_time(), "yyyyMMdd") + "_" + assetId + "_" + nameCN;
        byte[] destKey = key.getBytes();
        Boolean isExists = jedis.exists(destKey);
        if (isExists) {
            String oldDataJson = (String) KyroSerialUtil.deserialize(jedis.get(destKey));
            //取value最大值
            boolean result = getMaxValue(newJsonData, oldDataJson);
            if (result) {
                jedis.set(destKey, KyroSerialUtil.serialize(newJsonData));
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("update data to redis,destKey:{},oldDataJson:{},newJsonData:{}", destKey, oldDataJson, newJsonData);
                } else {
                    LOGGER.info("update data to redis,destKey:{}", destKey);
                }
            }
        } else {
            jedis.set(destKey, KyroSerialUtil.serialize(newJsonData));
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("save data to redis,destKey:{},newJsonData:{}", destKey, newJsonData);
            } else {
                LOGGER.info("save data to redis,destKey:{}", destKey);
            }
        }
        //设置key失效时间为48h
        jedis.expire(destKey, 2 * 24 * 60 * 60);
    }

    /**
     * 判断json字符串中哪个value值大
     *
     * @param newJsonData
     * @param oldJsonData
     * @return
     */
    private boolean getMaxValue(String newJsonData, String oldJsonData) {

        boolean result = true;
        if (StringUtils.isNotEmpty(oldJsonData) && StringUtils.isEmpty(newJsonData)) {
            result = false;
        } else if (StringUtils.isNotEmpty(newJsonData) && StringUtils.isNotEmpty(oldJsonData)) {
            Gson gson = new Gson();
            AlarmDto newAlarmDto = gson.fromJson(newJsonData, AlarmDto.class);
            AlarmDto oldAlarmDto = gson.fromJson(oldJsonData, AlarmDto.class);
            String newValue = newAlarmDto.getRealValue();
            String oldValue = oldAlarmDto.getRealValue();

            if (!isNumeric(newValue) || !isNumeric(oldValue)) {
                return false;
            }
            int code = Double.compare(Double.parseDouble(newValue), Double.parseDouble(oldValue));
            result = code > 0 ? true : false;
        }
        return result;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (null != jedis) {
            jedis.close();
        }
    }

    private String timeStamp2Date(String time, String format) {

        if (StringUtils.isBlank(time)) {
            return "";
        }
        if (StringUtils.isEmpty(format)) {
            format = "yyyy-MM-dd HH:mm:ss";
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        return dateFormat.format(new Date(Long.valueOf(time)));
    }

    private boolean isNumeric(String value) {

        if (StringUtils.isEmpty(value)) {
            return false;
        }
        return value.matches("(^-?[0-9][0-9]*(.[0-9]+)?)$");
    }

}
