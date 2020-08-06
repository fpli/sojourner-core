package com.ebay.sojourner.flink.common.util;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.model.UbiSession;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

public class OutputTagConstants {

  public static OutputTag<UbiSession> sessionOutputTag =
      new OutputTag<>("session-output-tag", TypeInformation.of(UbiSession.class));

  public static OutputTag<UbiEvent> lateEventOutputTag =
      new OutputTag<>("late-event-output-tag", TypeInformation.of(UbiEvent.class));

  public static OutputTag<UbiEvent> mappedEventOutputTag =
      new OutputTag<>("mapped-event-output-tag", TypeInformation.of(UbiEvent.class));

  public static OutputTag<RawEvent> dataSkewOutputTag =
      new OutputTag<>("skew-raw-event-output-tag", TypeInformation.of(RawEvent.class));
}
