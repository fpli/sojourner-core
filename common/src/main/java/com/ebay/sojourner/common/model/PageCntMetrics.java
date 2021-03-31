package com.ebay.sojourner.common.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class PageCntMetrics extends TdqMetrics implements Serializable {
    private Map<String, Map<Integer, Long>> pageCntMap = new HashMap<>();
}
