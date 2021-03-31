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
public class TotalCntMetrics extends TdqMetrics implements Serializable {
    private Map<String, Long> totalCntMap= new HashMap<>();
    private Map<String, Long> totalCntItmMap= new HashMap<>();
}