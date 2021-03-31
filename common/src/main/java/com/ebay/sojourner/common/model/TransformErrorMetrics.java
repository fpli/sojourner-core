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
public class TransformErrorMetrics extends TdqMetrics implements Serializable {
    private Map<String,Map<String,Long>> tagErrorCntMap= new HashMap<>();
}
