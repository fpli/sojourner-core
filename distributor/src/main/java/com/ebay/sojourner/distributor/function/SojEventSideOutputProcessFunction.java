package com.ebay.sojourner.distributor.function;

import static com.ebay.sojourner.flink.common.OutputTagConstants.SOJ_EVENT_NON_BOT;

import com.ebay.sojourner.common.model.SojEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class SojEventSideOutputProcessFunction extends ProcessFunction<SojEvent, SojEvent> {

  @Override
  public void processElement(SojEvent value, Context ctx,
                             Collector<SojEvent> out) throws Exception {

    // emit regular events
    out.collect(value);

    // emit nonbot events to side output
    if (value.getBot() == 0) {
      ctx.output(SOJ_EVENT_NON_BOT, value);
    }
  }
}
