package com.zlst.entity;

import java.io.Serializable;

import com.zlst.enums.LogType;

import lombok.Data;
/**
 * 日志信息基类
 * @author 170213
 *
 */
@Data
@SuppressWarnings("serial")
public class LogBaseData implements Serializable {
	
	private long logTime;
	
	private String hostName;
}
