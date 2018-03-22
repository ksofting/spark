package com.zlst.enums;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 
 * 日志类型
 * 
 * @author 170213 2018年3月21日
 * @see
 * @since 1.0
 */
public enum LogType{
    /**
     * 容器
     */
    CONTAINER("docker"),
    /**
     * 文件系统
     */
    FILESSYSTEM("filesystem");
    
    private static Map<String,LogType> valMap= new LinkedHashMap<String,LogType>();
    
    private static boolean inited= false;
    /**
     * 初始化方法
     *
     */
    static{
        if(! inited){
            LogType[] ins= LogType.values();
            for(int i= 0;i< ins.length;i++ ){
                valMap.put(ins[i].getValue(),ins[i]);
            }
            inited= true;
        }
    }
    
    private String value;
    
    private LogType(String value){
        
        this.value= value;
    }
    
    public String getValue(){
        
        return value;
    }
    
    public void setValue(String value){
        
        this.value= value;
    }
    
    public static LogType getLogType(String v){
        
        return valMap.get(v);
    }
}
