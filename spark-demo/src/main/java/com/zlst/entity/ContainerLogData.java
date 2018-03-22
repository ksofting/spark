package com.zlst.entity;

/**
 * 容器日志
 */
import lombok.Data;
import lombok.EqualsAndHashCode;
@Data
@EqualsAndHashCode(callSuper=false)
@SuppressWarnings("serial")
public class ContainerLogData extends LogBaseData {
	
	private String containerId;
	private String containerName;
	private long limit;
	private long rssTotal;
	private double rssPct;
	private long usageMax;
	private long usageTotal;
	private double usagePct;
}
