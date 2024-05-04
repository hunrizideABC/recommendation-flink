package com.recflink.map;

import com.recflink.domain.LogEntity;
import com.recflink.util.LogToEntity;
import org.apache.flink.api.common.functions.MapFunction;

/*
 * 将kafka 的数据 转为 Log类
 */
public class GetLogFunction implements MapFunction<String, LogEntity> {
    @Override
    public LogEntity map(String s) throws Exception {

        LogEntity log = LogToEntity.getLog(s);
        return log;
    }
}
