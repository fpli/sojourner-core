package com.ebay.sojourner.flink.common.util;

import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.IntermediateMetrics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

public class FlinkTypeCheckUtil {

  public static void main(String[] args) {
    TypeInformation<UbiSession> ubiSessionTypeInformation = TypeExtractor
        .createTypeInfo(UbiSession.class);
    System.out.println(ubiSessionTypeInformation);
    TypeInformation<IntermediateMetrics> intermediateMetricsTypeInformation1 = TypeInformation
        .of(IntermediateMetrics.class);
    System.out.println(intermediateMetricsTypeInformation1);
    TypeInformation<UbiEvent> ubiEventTypeInformation = TypeInformation
        .of(UbiEvent.class);
    System.out.println(ubiEventTypeInformation);
  }
}