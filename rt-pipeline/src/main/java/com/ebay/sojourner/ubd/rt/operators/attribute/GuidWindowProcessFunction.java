package com.ebay.sojourner.ubd.rt.operators.attribute;

import com.ebay.sojourner.ubd.common.model.GuidAttribute;
import com.ebay.sojourner.ubd.common.model.GuidAttributeAccumulator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class GuidWindowProcessFunction
    extends ProcessWindowFunction<
    GuidAttributeAccumulator, Tuple4<String, Boolean, Set<Integer>, Long>, Tuple, TimeWindow> {

  @Override
  public void process(
      Tuple tuple,
      Context context,
      Iterable<GuidAttributeAccumulator> elements,
      Collector<Tuple4<String, Boolean, Set<Integer>, Long>> out)
      throws Exception {

    GuidAttributeAccumulator guidAttributeAccumulator = elements.iterator().next();
    GuidAttribute guidAttribute = guidAttributeAccumulator.getGuidAttribute();

    if (context.currentWatermark() >= context.window().maxTimestamp()
        && guidAttribute.getBotFlagList() != null
        && guidAttribute.getBotFlagList().size() > 0) {
      out.collect(
          new Tuple4<>(
              "guid" + guidAttribute.getGuid1() + guidAttribute.getGuid2(),
              false,
              guidAttribute.getBotFlagList(),
              context.window().maxTimestamp()));
    } else if (context.currentWatermark() < context.window().maxTimestamp()
        && guidAttributeAccumulator.getBotFlagStatus().containsValue(1)
        && guidAttribute.getBotFlagList() != null
        && guidAttribute.getBotFlagList().size() > 0) {
      HashSet<Integer> generationBotFlag = new HashSet<>();
      for (Map.Entry<Integer, Integer> newBotFlagMap :
          guidAttributeAccumulator.getBotFlagStatus().entrySet()) {
        if (newBotFlagMap.getValue() == 1) {
          generationBotFlag.add(newBotFlagMap.getKey());
        }
      }
      out.collect(
          new Tuple4<>(
              "guid" + guidAttribute.getGuid1() + guidAttribute.getGuid2(),
              true,
              generationBotFlag,
              context.window().maxTimestamp()));
    }
  }

  @Override
  public void open(Configuration conf) throws Exception {
    super.open(conf);
  }

  @Override
  public void clear(Context context) throws Exception {
    super.clear(context);
  }
}
