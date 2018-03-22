package com.zlst.entity;

import java.io.Serializable;
import java.util.List;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DataInfo implements Serializable{

    private static final long serialVersionUID= 8649808144749629511L;
    private String scrollId;
    private List<String> dataList;
}
