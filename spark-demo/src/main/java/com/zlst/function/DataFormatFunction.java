package com.zlst.function;

import java.io.Serializable;
import java.text.ParseException;

import org.apache.arrow.flatbuf.DateUnit;
import org.apache.spark.api.java.function.Function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.framework.common.utils.DateUtils;
import com.framework.common.utils.StringUtils;
import com.zlst.entity.ContainerLogData;
import com.zlst.entity.LogInfo;
import com.zlst.enums.LogType;

import scala.Tuple2;

/**
 * 数据转换函数
 * 
 * @author 170213
 *
 */
@SuppressWarnings("serial")
public class DataFormatFunction implements Serializable{
    
    public static LogInfo call(Tuple2<String,String> tuple)
        throws Exception{
        
        JSONObject jsonObject= JSON.parseObject(tuple._2);
        JSONObject sourceObject= jsonObject.getJSONObject("_source");
        if(sourceObject.containsKey("error")){//错误日志
            return LogInfo.EmptyInstance();
        }
        if(sourceObject.containsKey("docker")){
            return new LogInfo(LogType.CONTAINER,parseDocker(sourceObject));
        }
        else{
            return LogInfo.EmptyInstance();
        }
    }
    
    private static ContainerLogData parseDocker(JSONObject sourceObject)
        throws ParseException{
        
        JSONObject dockerObject= sourceObject.getJSONObject("docker");
        JSONObject beatObject= sourceObject.getJSONObject("beat");
        JSONObject rssObject= dockerObject.getJSONObject("memory").getJSONObject("rss");
        JSONObject usageObject= dockerObject.getJSONObject("memory").getJSONObject("usage");
        JSONObject containerObject= dockerObject.getJSONObject("container");
        
        ContainerLogData data= new ContainerLogData();
        data.setLogTime(DateUtils.parseUTCDateTime(sourceObject.getString("@timestamp")).getTime());
        data.setHostName(beatObject.getString("hostname"));
        data.setLimit(dockerObject.getLongValue("limit"));
        data.setRssTotal(rssObject.getLongValue("total"));
        data.setRssPct(rssObject.getDoubleValue("pct"));
        data.setUsageMax(usageObject.getLongValue("max"));
        data.setUsageTotal(usageObject.getLongValue("total"));
        data.setUsagePct(usageObject.getDoubleValue("pct"));
        data.setContainerId(containerObject.getString("id"));
        data.setContainerName(containerObject.getString("name"));
        return data;
    }
}
