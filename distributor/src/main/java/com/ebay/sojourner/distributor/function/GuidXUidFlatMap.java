package com.ebay.sojourner.distributor.function;

import com.ebay.sojourner.common.model.RawSojEventWrapper;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.common.util.Constants;
import com.ebay.sojourner.flink.connector.kafka.AvroKafkaDeserializer;
import com.ebay.sojourner.flink.connector.kafka.KafkaDeserializer;
import lombok.Data;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;

import java.util.Map;

@Data
public class GuidXUidFlatMap extends RichFlatMapFunction<RawSojEventWrapper, SojEvent> {

    private transient KafkaDeserializer<SojEvent> deserializer;

    private Counter collectEventCounter;
    private Counter dropEventCounter;
    private Counter invalidUserIdEventCounter;
    private Counter invalidBuEventCounter;
    private Counter invalidUserCounter;
    private static final String COLLECT_EVENT_METRIC_NAME = "prod-collect-event-count";
    private static final String DROP_EVENT_METRIC_NAME = "prod-drop-event-count";
    private static final String INVALID_USER_ID_EVENT_METRIC_NAME = "invalid-user-id-event-count";
    private static final String INVALID_BU_EVENT_METRIC_NAME = "invalid-bu-event-count";
    private static final String INVALID_USER_EVENT_METRIC_NAME = "invalid-user-event-count";

    private boolean checkUserId(String uid) {
        if (uid == null || uid.length() > 10) {
            return false;
        }
        try {
            long i = Long.parseLong(uid);
            if (i == 0 || i == -1) {
                return false;
            }
        } catch (NumberFormatException exception) {
            invalidUserCounter.inc();
            return false;
        }
        return true;
    }

    public boolean filter(SojEvent sojEvent) throws Exception {
        String guid = sojEvent.getGuid();
        if (null == guid || !guid.replaceAll("[0123456789abcdefABCDEF]", "").trim().isEmpty()) {
            return false;
        }
        Integer rdt = sojEvent.getRdt();
        if (0 != rdt) {
            return false;
        }
        Boolean iframe = sojEvent.getIframe();
        if (iframe != null && iframe) {
            return false;
        }
        Map<String, String> applicationPayload = sojEvent.getApplicationPayload();
        String bu = applicationPayload.get("bu");
        boolean buValid = checkUserId(bu);
        if (!buValid) {
            invalidUserIdEventCounter.inc();
            //  applicationPayload.replace("bu", null);
            applicationPayload.remove("bu");
        }
        String userId = sojEvent.getUserId();
        boolean userIdValid = checkUserId(userId);
        if (!userIdValid) {
            invalidBuEventCounter.inc();
            sojEvent.setUserId(null);
        }
        return buValid || userIdValid;
    }

    @Override
    public void flatMap(RawSojEventWrapper sojEventWrapper, Collector<SojEvent> out) throws Exception {
        byte[] payload = sojEventWrapper.getValue();
        SojEvent sojEvent = deserializer.decodeValue(payload);
        if (filter(sojEvent)){
            collectEventCounter.inc();
            out.collect(sojEvent);
        } else {
            dropEventCounter.inc();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.deserializer = new AvroKafkaDeserializer<>(SojEvent.class);
        collectEventCounter = getRuntimeContext()
                                .getMetricGroup()
                                .addGroup(Constants.SOJ_METRICS_GROUP)
                                .counter(COLLECT_EVENT_METRIC_NAME);
        dropEventCounter = getRuntimeContext()
                                .getMetricGroup()
                                .addGroup(Constants.SOJ_METRICS_GROUP)
                                .counter(DROP_EVENT_METRIC_NAME);
        invalidUserIdEventCounter = getRuntimeContext()
                                .getMetricGroup()
                                .addGroup(Constants.SOJ_METRICS_GROUP)
                                .counter(INVALID_USER_ID_EVENT_METRIC_NAME);
        invalidBuEventCounter = getRuntimeContext()
                                .getMetricGroup()
                                .addGroup(Constants.SOJ_METRICS_GROUP)
                                .counter(INVALID_BU_EVENT_METRIC_NAME);
        invalidUserCounter = getRuntimeContext()
                                .getMetricGroup()
                                .addGroup(Constants.SOJ_METRICS_GROUP)
                                .counter(INVALID_USER_EVENT_METRIC_NAME);
    }
}
