package com.ebay.sojourner.flink.common;

import com.ebay.sojourner.common.model.*;
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

    public static OutputTag<SojEvent> botEventOutputTag =
            new OutputTag<>("bot-event-output-tag", TypeInformation.of(SojEvent.class));

    public static OutputTag<SojSession> botSessionOutputTag =
            new OutputTag<>("bot-session-output-tag", TypeInformation.of(SojSession.class));

    public static OutputTag<SojSession> crossDaySessionOutputTag =
            new OutputTag<>("cross-day-session-output-tag", TypeInformation.of(SojSession.class));

    public static OutputTag<SojSession> openSessionOutputTag =
            new OutputTag<>("open-session-output-tag", TypeInformation.of(SojSession.class));

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

}
