package com.ebay.sojourner.rt.operator.session;

import com.ebay.sojourner.common.model.SojSession;
import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.SojUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class UbiSessionToSojSessionProcessFunction extends ProcessFunction<UbiSession, SojSession> {

  private final OutputTag<SojSession> outputTag;

  public UbiSessionToSojSessionProcessFunction(OutputTag<SojSession> outputTag) {
    this.outputTag = outputTag;
  }

  @Override
  public void processElement(UbiSession ubiSession, Context context, Collector<SojSession> out)
      throws Exception {
    SojSession sojSession = SojUtils.convertUbiSession2SojSession(ubiSession);

    // split bot session and nonbot session
    if (sojSession.getBotFlagList().size() == 0 || CollectionUtils
        .subtract(sojSession.getBotFlagList(), SojUtils.getIntermediateBotFlagList()).size() == 0) {
      out.collect(sojSession);
    } else {
      context.output(outputTag, sojSession);
    }
  }
}
