package com.ebay.sojourner.rt.operator.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

import com.ebay.sojourner.business.detector.NewEventBotDetector;
import com.ebay.sojourner.business.parser.EventParser;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import org.apache.flink.api.common.accumulators.AverageAccumulator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.powermock.reflect.Whitebox;

class EventMapFunctionTest {

  EventMapFunction mapFunction;
  RuntimeContext mockRuntimeContext = mock(RuntimeContext.class);
  EventParser mockEventParser = mock(EventParser.class);
  NewEventBotDetector newEventBotDetector = mock(NewEventBotDetector.class);

  @BeforeEach
  void setUp() {
    mapFunction = new EventMapFunction();
    FunctionUtils.setFunctionRuntimeContext(mapFunction, mockRuntimeContext);
    doNothing().when(mockRuntimeContext).addAccumulator(anyString(), any());
  }

  @Test
  void open() throws Exception {
    mapFunction.open(new Configuration());
    EventParser parser = Whitebox.getInternalState(mapFunction, "parser");
    assertThat(parser).isNotNull();
  }

  @Test
  void map() throws Exception {
    RawEvent rawEvent = new RawEvent();
    AverageAccumulator avgEventParserDuration = new AverageAccumulator();
    Whitebox.setInternalState(mapFunction, "parser", mockEventParser);
    Whitebox.setInternalState(mapFunction, "avgEventParserDuration", avgEventParserDuration);
    Whitebox.setInternalState(mapFunction, "newEventBotDetector", newEventBotDetector);
    doNothing().when(mockEventParser).parse(any(), any());
    UbiEvent ubiEvent = mapFunction.map(rawEvent);
    assertThat(ubiEvent).isNotNull();
    assertThat(ubiEvent.getBotFlags().size()).isEqualTo(0);
  }
}