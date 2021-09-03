package com.ebay.sojourner.rt.operator.event;

import com.ebay.sojourner.business.detector.NewEventBotDetector;
import com.ebay.sojourner.business.parser.EventParser;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.accumulators.AverageAccumulator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

@Slf4j
public class EventMapFunction extends RichMapFunction<RawEvent, UbiEvent> {

  private EventParser parser;
  private AverageAccumulator avgEventParserDuration;
  private NewEventBotDetector newEventBotDetector;

  @Override
  public void open(Configuration conf) throws Exception {
    super.open(conf);
    parser = new EventParser();
    avgEventParserDuration = new AverageAccumulator();
    newEventBotDetector = new NewEventBotDetector();
    getRuntimeContext()
        .addAccumulator("Average Duration of Event Parsing", avgEventParserDuration);
  }

  @Override
  public UbiEvent map(RawEvent rawEvent) throws Exception {
    UbiEvent event = new UbiEvent();
    long startTimeForEventParser = System.nanoTime();
    parser.parse(rawEvent, event);
    avgEventParserDuration.add(System.nanoTime() - startTimeForEventParser);
    Set<Integer> eventBotFlagList = newEventBotDetector.getBotFlagList(event);
    event.getBotFlags().addAll(eventBotFlagList);
    return event;
  }
}
