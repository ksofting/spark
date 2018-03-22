package com.zlst;

import java.io.FileInputStream;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Test{
    
    public static void main(String[] args)
        throws Exception{
        
        ObjectMapper mapper= new ObjectMapper();
        Map map= mapper.readValue(new FileInputStream("e:/json1.txt"),Map.class);
        System.out.println(map);
    }
}
