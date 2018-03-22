package com.zlst;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.google.common.collect.Lists;

/**
 * @ClassName: ClientChannel
 * @Description: 客户端连接
 * @author zhangxiaohui 2017年11月28日 下午2:16:29
 * 
 */
public class ClientChannel{
    
    private TransportClient client;
    
    public ClientChannel(){
        
        open();
    }
    
    @SuppressWarnings("resource")
    private void open(){
        
        Settings settings= Settings.builder().put("cluster.name","elasticsearch").put("client.transport.sniff",true).build();
        TransportAddress[] addresses= new TransportAddress[]{
            new TransportAddress(new InetSocketAddress("192.168.10.170",9300)),new TransportAddress(new InetSocketAddress("192.168.10.171",9300)),new TransportAddress(
                new InetSocketAddress("192.168.10.172",9300))
        };
        client= new PreBuiltTransportClient(settings).addTransportAddresses(addresses);
    }
    
    public TransportClient getClient(){
        
        return client;
    }
    
    public void close(){
        
        if(client== null){
            return;
        }
        client.close();
    }
    
    /**
     * @Title: createSearch
     * @Description: 获取批量查询对象
     * @param @param index
     * @param @param type
     * @param @param queryBuilder
     * @param @param size
     * @param @return
     * @return SearchResponse
     * @throws @author zhangxiaohui 2017年11月28日 下午4:21:55
     */
    public SearchResponse createSearch(String index,String type,QueryBuilder queryBuilder,int size,boolean isScroll){
        
        SearchRequestBuilder search= client.prepareSearch(index.toLowerCase()).setTypes(type.toLowerCase()).setQuery(queryBuilder);
        if(isScroll){
            search.setScroll(TimeValue.timeValueMinutes(1));
        }
        if(size> 0){
            search.setSize(size);
        }
        return search.execute().actionGet();
    }
    
    public List<String> getDataByScroll(String scrollId){
        
        try{
            SearchResponse searchResponse= client.prepareSearchScroll(scrollId).setScroll(TimeValue.timeValueMinutes(1)).execute().actionGet();
            SearchHits hits= searchResponse.getHits();
            Iterator<SearchHit> iter= hits.iterator();
            List<String> returnList= Lists.newArrayList();
            while(iter.hasNext()){
                returnList.add(iter.next().getSourceAsString());
            }
            return returnList;
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }
}
