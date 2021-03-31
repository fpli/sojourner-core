package com.ebay.sojourner.rt.operator.event;

import static org.assertj.core.api.Assertions.assertThat;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.flink.common.OutputTagConstants;
import java.util.List;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UbiEventToSojEventProcessFunctionTest {

  UbiEventToSojEventProcessFunction processFunction;

  @BeforeEach
  void setUp() {
    processFunction = new UbiEventToSojEventProcessFunction(OutputTagConstants.botEventOutputTag);
  }

  @Test
  void processElement() throws Exception {
    // wrap user defined function into a the corresponding operator
    OneInputStreamOperatorTestHarness<UbiEvent, SojEvent> harness = ProcessFunctionTestHarnesses
        .forProcessFunction(processFunction);
    UbiEvent ubiEvent = new UbiEvent();
    ubiEvent.setEventTimestamp(1000000L);
    ubiEvent.setSessionStartDt(1000000L);
    ubiEvent.setSojDataDt(100000L);
    harness.processElement(ubiEvent, 10);
    List<SojEvent> sojEvents = harness.extractOutputValues();
    assertThat(sojEvents.size()).isEqualTo(1);
  }
}