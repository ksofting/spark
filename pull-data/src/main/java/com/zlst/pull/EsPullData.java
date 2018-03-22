package com.zlst.pull;

import java.util.Iterator;
import java.util.List;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import com.framework.common.utils.StringUtils;
import com.google.common.collect.Lists;
import com.zlst.ClientChannel;
import com.zlst.entity.DataInfo;

public class EsPullData{
    
    private ClientChannel channel;
    
    public EsPullData(){
        
        channel= new ClientChannel();
    }
    
    public DataInfo pullData(String scrollId){
        
        List<String> returnList;
        if(StringUtils.isEmpty(scrollId)){
            SearchResponse response= channel.createSearch("metricbeat-2018.03.20","doc",QueryBuilders.matchAllQuery(),100,true);
            scrollId= response.getScrollId();
            SearchHits hits= response.getHits();
            Iterator<SearchHit> iter= hits.iterator();
            returnList= Lists.newArrayList();
            while(iter.hasNext()){
                returnList.add(iter.next().getSourceAsString());
            }
        }
        else{
            returnList= channel.getDataByScroll(scrollId);
        }
        return DataInfo.builder().scrollId(scrollId).dataList(returnList).build();
    }
    
    public static void main(String[] args){
        
        EsPullData bean= new EsPullData();
        DataInfo dataInfo= bean.pullData(null);
        System.out.println(dataInfo.getScrollId());
        for(String string : dataInfo.getDataList()){
            System.out.println(string);
        }
        dataInfo= bean.pullData(dataInfo.getScrollId());
        System.out.println("\n");
        for(String string : dataInfo.getDataList()){
            System.out.println(string);
        }
    }
}
