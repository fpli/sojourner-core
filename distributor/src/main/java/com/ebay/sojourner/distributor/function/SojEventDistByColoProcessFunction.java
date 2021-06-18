package com.ebay.sojourner.distributor.function;

import com.ebay.sojourner.common.model.SimpleDistSojEventWrapper;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SojEventDistByColoProcessFunction extends
    ProcessFunction<SimpleDistSojEventWrapper, SimpleDistSojEventWrapper> {

  private final List<String> keyList;
  private final List<String> dcList;
  private final Map<String, OutputTag<SimpleDistSojEventWrapper>> outputTagMap = Maps.newHashMap();

  public SojEventDistByColoProcessFunction(
      List<String> keyList, List<String> dcList) {
    this.keyList = keyList;
    this.dcList = dcList;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
  }

  @Override
  public void processElement(SimpleDistSojEventWrapper simpleDistSojEventWrapper, Context ctx,
      Collector<SimpleDistSojEventWrapper> out) throws Exception {

    if (CollectionUtils.isNotEmpty(keyList) && CollectionUtils.isNotEmpty(dcList)) {
      Integer hashCode = SimpleDistSojEventPartitioner.hash(simpleDistSojEventWrapper, keyList);
      if (hashCode != null) {
        int index = Math.abs(hashCode % dcList.size());
        String dc = dcList.get(index);
        if (!outputTagMap.containsKey(dc)) {
          outputTagMap.put(dc, new OutputTag<>("simple-distribution-to-" + dc,
              TypeInformation.of(SimpleDistSojEventWrapper.class)));
        }
        ctx.output(outputTagMap.get(dc), simpleDistSojEventWrapper);
      }
    }
  }
}
