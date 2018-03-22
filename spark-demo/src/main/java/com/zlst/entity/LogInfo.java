package com.zlst.entity;

import java.io.Serializable;

import com.zlst.enums.LogType;

import lombok.Data;
/**
 * 日志信息 
 * @author 170213 2018年3月21日
 * @see
 * @since 1.0
 */
@Data
@SuppressWarnings("serial")
public class LogInfo implements Serializable{
    
    private LogType logType;
    
    private LogBaseData data;
    public LogInfo() {
    }
    public static LogInfo EmptyInstance() {
        return new LogInfo();
    }
    public LogInfo(LogType logType,LogBaseData data) {
        this.logType=logType;
        this.data=data;
    }
}
