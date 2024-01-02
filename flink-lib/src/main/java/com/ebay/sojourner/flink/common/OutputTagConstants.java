package com.ebay.sojourner.flink.common;

import com.ebay.sojourner.common.model.SessionMetrics;
import com.ebay.sojourner.common.model.PageCntMetrics;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.SimpleDistSojEventWrapper;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.common.model.SojSession;
import com.ebay.sojourner.common.model.TagMissingCntMetrics;
import com.ebay.sojourner.common.model.TagSumMetrics;
import com.ebay.sojourner.common.model.TotalCntMetrics;
import com.ebay.sojourner.common.model.TransformErrorMetrics;
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

  public static OutputTag<SojEvent> SOJ_EVENT_NON_BOT =
      new OutputTag<>("sojevent-nonbot", TypeInformation.of(SojEvent.class));

  public static OutputTag<SojEvent> botEventOutputTag =
      new OutputTag<>("bot-event-output-tag", TypeInformation.of(SojEvent.class));

  public static OutputTag<SojSession> botSessionOutputTag =
      new OutputTag<>("bot-session-output-tag", TypeInformation.of(SojSession.class));

  public static OutputTag<SojSession> crossDaySessionOutputTag =
      new OutputTag<>("cross-day-session-output-tag", TypeInformation.of(SojSession.class));

  public static OutputTag<SojSession> openSessionOutputTag =
      new OutputTag<>("open-session-output-tag", TypeInformation.of(SojSession.class));

  public static OutputTag<SessionMetrics> sessionMetricsOutputTag =
          new OutputTag<>("session-metrics-output-tag", TypeInformation.of(SessionMetrics.class));
  public static OutputTag<SessionMetrics> crossDaySessionMetricsOutputTag =
          new OutputTag<>("cross-day-session-metrics-output-tag", TypeInformation.of(SessionMetrics.class));

  public static OutputTag<SessionMetrics> openSessionMetricsOutputTag =
          new OutputTag<>("open-session-metrics-output-tag", TypeInformation.of(SessionMetrics.class));
  public static final OutputTag<TagMissingCntMetrics> TAG_MISSING_CNT_METRICS_OUTPUT_TAG =
      new OutputTag<>("tagMissingCntMetrics",
          TypeInformation.of(TagMissingCntMetrics.class));
  public static final OutputTag<TagSumMetrics> TAG_SUM_METRICS_OUTPUT_TAG =
      new OutputTag<>("tagSumMetrics", TypeInformation.of(TagSumMetrics.class));
  public static final OutputTag<PageCntMetrics> PAGE_CNT_METRICS_OUTPUT_TAG =
      new OutputTag<>("pageCntMetrics", TypeInformation.of(PageCntMetrics.class));
  public static final OutputTag<TransformErrorMetrics> TRANSFORM_ERROR_METRICS_OUTPUT_TAG =
      new OutputTag<>("transformerrorMetrics",
          TypeInformation.of(TransformErrorMetrics.class));
  public static final OutputTag<TotalCntMetrics> TOTAL_CNT_METRICS_OUTPUT_TAG =
      new OutputTag<>("totalCntMetrics",
          TypeInformation.of(TotalCntMetrics.class));
  public static OutputTag<SimpleDistSojEventWrapper> rnoDistOutputTag =
      new OutputTag<>("simple-distribution-to-rno",
          TypeInformation.of(SimpleDistSojEventWrapper.class));
  public static OutputTag<SimpleDistSojEventWrapper> lvsDistOutputTag =
      new OutputTag<>("simple-distribution-to-lvs",
          TypeInformation.of(SimpleDistSojEventWrapper.class));
  public static OutputTag<SimpleDistSojEventWrapper> slcDistOutputTag =
      new OutputTag<>("simple-distribution-to-slc",
          TypeInformation.of(SimpleDistSojEventWrapper.class));

}
